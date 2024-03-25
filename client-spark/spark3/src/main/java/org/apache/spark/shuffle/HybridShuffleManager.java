/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.request.RssAccessClusterRequest;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;

import static org.apache.uniffle.common.util.Constants.ACCESS_INFO_REQUIRED_SHUFFLE_NODES_NUM;

public class HybridShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(HybridShuffleManager.class);

  private final int accessTimeoutMs;
  private final SparkConf sparkConf;
  private ShuffleManager essShuffleManager;
  private ShuffleManager rssShuffleManager;
  private List<CoordinatorClient> coordinatorClients;
  private Set<Integer> rssShuffleIds;
  private boolean isDriver;
  private String user;
  private String uuid;

  public HybridShuffleManager(SparkConf sparkConf, boolean isDriver) throws Exception {
    this.sparkConf = sparkConf;
    this.isDriver = isDriver;
    accessTimeoutMs = sparkConf.get(RssSparkConfig.RSS_ACCESS_TIMEOUT_MS);

    coordinatorClients = createCoordinatorClients(isDriver, sparkConf);
    essShuffleManager = null;
    rssShuffleManager = null;

    rssShuffleIds = Sets.newConcurrentHashSet();
  }

  private List<CoordinatorClient> createCoordinatorClients(boolean isDriver, SparkConf sparkConf) {
    List<CoordinatorClient> coordinatorClients;
    if (isDriver) {
      coordinatorClients = RssSparkShuffleUtils.createCoordinatorClients(sparkConf);
    } else {
      coordinatorClients = Lists.newArrayList();
    }
    return coordinatorClients;
  }

  private ShuffleManager createEssShuffleManager(SparkConf sparkConf, boolean isDriver)
      throws RssException {
    ShuffleManager shuffleManager;

    try {
      shuffleManager =
          RssSparkShuffleUtils.loadShuffleManager(
              Constants.SORT_SHUFFLE_MANAGER_NAME, sparkConf, true);
      if (isDriver) {
        sparkConf.set(RssSparkConfig.RSS_ENABLED.key(), "false");
        sparkConf.set("spark.shuffle.manager", "sort");
      }
    } catch (Exception e) {
      throw new RssException(e.getMessage());
    }

    return shuffleManager;
  }

  private ShuffleManager createRssShuffleManager(SparkConf sparkConf, boolean isDriver)
      throws RssException {
    ShuffleManager shuffleManager = null;
    if (isDriver) {
      user = "user";
      try {
        user = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (Exception e) {
        LOG.error("Error on getting user from ugi." + e);
      }
      boolean canAccess = tryAccessCluster();
      if (uuid == null || "".equals(uuid)) {
        uuid = String.valueOf(System.currentTimeMillis());
      }

      if (canAccess) {
        try {
          sparkConf.set("spark.rss.quota.user", user);
          sparkConf.set("spark.rss.quota.uuid", uuid);
          shuffleManager = new RssShuffleManager(sparkConf, true);
          sparkConf.set(RssSparkConfig.RSS_ENABLED.key(), "true");
          sparkConf.set("spark.shuffle.manager", RssShuffleManager.class.getCanonicalName());
          LOG.info("Use RssShuffleManager");
          return shuffleManager;
        } catch (Exception exception) {
          LOG.warn(
              "Fail to create RssShuffleManager, fallback to SortShuffleManager {}",
              exception.getMessage());
        }
      }
    } else {
      boolean useRSS = sparkConf.get(RssSparkConfig.RSS_ENABLED);
      if (useRSS) {
        // Executor will not do any fallback
        try {
          shuffleManager = new RssShuffleManager(sparkConf, false);
          LOG.info("Use RssShuffleManager");
        } catch (Exception exception) {
          LOG.warn(
              "Fail to create RssShuffleManager, fallback to SortShuffleManager {}",
              exception.getMessage());
        }
      }
    }
    return shuffleManager;
  }

  private boolean tryAccessCluster() {
    String accessId = sparkConf.get(RssSparkConfig.RSS_ACCESS_ID.key(), "").trim();
    if (StringUtils.isEmpty(accessId)) {
      LOG.warn("Access id key is empty");
      return false;
    }
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ACCESS_RETRY_INTERVAL_MS);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ACCESS_RETRY_TIMES);

    int assignmentShuffleNodesNum =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
    Map<String, String> extraProperties = Maps.newHashMap();
    extraProperties.put(
        ACCESS_INFO_REQUIRED_SHUFFLE_NODES_NUM, String.valueOf(assignmentShuffleNodesNum));

    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
      boolean canAccess;
      try {
        canAccess =
            RetryUtils.retry(
                () -> {
                  RssAccessClusterResponse response =
                      coordinatorClient.accessCluster(
                          new RssAccessClusterRequest(
                              accessId, assignmentTags, accessTimeoutMs, extraProperties, user));
                  if (response.getStatusCode() == StatusCode.SUCCESS) {
                    LOG.warn(
                        "Success to access cluster {} using {}",
                        coordinatorClient.getDesc(),
                        accessId);
                    uuid = response.getUuid();
                    return true;
                  } else if (response.getStatusCode() == StatusCode.ACCESS_DENIED) {
                    throw new RssException(
                        "Request to access cluster "
                            + coordinatorClient.getDesc()
                            + " is denied using "
                            + accessId
                            + " for "
                            + response.getMessage());
                  } else {
                    throw new RssException(
                        "Fail to reach cluster "
                            + coordinatorClient.getDesc()
                            + " for "
                            + response.getMessage());
                  }
                },
                retryInterval,
                retryTimes);
        return canAccess;
      } catch (Throwable e) {
        LOG.warn(
            "Fail to access cluster {} using {} for {}",
            coordinatorClient.getDesc(),
            accessId,
            e.getMessage());
      }
    }

    return false;
  }

  private ShuffleManager getOrCreateEssShuffleManager() {
    if (essShuffleManager == null) essShuffleManager = createEssShuffleManager(sparkConf, isDriver);
    return essShuffleManager;
  }

  private ShuffleManager getOrCreateRssShuffleManager() {
    if (rssShuffleManager == null) rssShuffleManager = createRssShuffleManager(sparkConf, isDriver);
    return rssShuffleManager;
  }

  public ShuffleManager registerShuffleManager(int shuffleId) {
    // If create RssShuffleManager failed, we downgrade to ess shufflemanager
    if (getOrCreateRssShuffleManager() != null) {
      rssShuffleIds.add(shuffleId);
      return getOrCreateRssShuffleManager();
    }
    return getOrCreateEssShuffleManager();
  }

  public ShuffleManager getEssShuffleManager() {
    return essShuffleManager;
  }

  public ShuffleManager getRssShuffleManager() {
    return rssShuffleManager;
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // If create RssShuffleManager failed, we downgrade to ess shufflemanager
    return registerShuffleManager(shuffleId).registerShuffle(shuffleId, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    return rssShuffleIds.contains(handle.shuffleId())
        ? getOrCreateRssShuffleManager().getWriter(handle, mapId, context, metrics)
        : getOrCreateEssShuffleManager().getWriter(handle, mapId, context, metrics);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return rssShuffleIds.contains(handle.shuffleId())
        ? getOrCreateRssShuffleManager()
            .getReader(handle, startPartition, endPartition, context, metrics)
        : getOrCreateEssShuffleManager()
            .getReader(handle, startPartition, endPartition, context, metrics);
  }

  // The interface is only used for compatibility with spark 3.1.2
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    ShuffleReader<K, C> reader = null;
    try {
      reader =
          rssShuffleIds.contains(handle.shuffleId())
              ? (ShuffleReader<K, C>)
                  getOrCreateRssShuffleManager()
                      .getClass()
                      .getDeclaredMethod(
                          "getReader",
                          ShuffleHandle.class,
                          int.class,
                          int.class,
                          int.class,
                          int.class,
                          TaskContext.class,
                          ShuffleReadMetricsReporter.class)
                      .invoke(
                          handle,
                          startMapIndex,
                          endMapIndex,
                          startPartition,
                          endPartition,
                          context,
                          metrics)
              : (ShuffleReader<K, C>)
                  getOrCreateEssShuffleManager()
                      .getClass()
                      .getDeclaredMethod(
                          "getReader",
                          ShuffleHandle.class,
                          int.class,
                          int.class,
                          int.class,
                          int.class,
                          TaskContext.class,
                          ShuffleReadMetricsReporter.class)
                      .invoke(
                          handle,
                          startMapIndex,
                          endMapIndex,
                          startPartition,
                          endPartition,
                          context,
                          metrics);
    } catch (Exception e) {
      throw new RssException(e);
    }
    return reader;
  }

  // The interface is only used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    ShuffleReader<K, C> reader = null;
    try {
      reader =
          rssShuffleIds.contains(handle.shuffleId())
              ? (ShuffleReader<K, C>)
                  getOrCreateRssShuffleManager()
                      .getClass()
                      .getDeclaredMethod(
                          "getReaderForRange",
                          ShuffleHandle.class,
                          int.class,
                          int.class,
                          int.class,
                          int.class,
                          TaskContext.class,
                          ShuffleReadMetricsReporter.class)
                      .invoke(
                          handle,
                          startMapIndex,
                          endMapIndex,
                          startPartition,
                          endPartition,
                          context,
                          metrics)
              : (ShuffleReader<K, C>)
                  getOrCreateEssShuffleManager()
                      .getClass()
                      .getDeclaredMethod(
                          "getReaderForRange",
                          ShuffleHandle.class,
                          int.class,
                          int.class,
                          int.class,
                          int.class,
                          TaskContext.class,
                          ShuffleReadMetricsReporter.class)
                      .invoke(
                          handle,
                          startMapIndex,
                          endMapIndex,
                          startPartition,
                          endPartition,
                          context,
                          metrics);
    } catch (Exception e) {
      throw new RssException(e);
    }
    return reader;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return rssShuffleIds.contains(shuffleId)
        ? getOrCreateRssShuffleManager().unregisterShuffle(shuffleId)
        : getOrCreateEssShuffleManager().unregisterShuffle(shuffleId);
  }

  @Override
  public void stop() {
    if (getOrCreateEssShuffleManager() != null) {
      getOrCreateEssShuffleManager().stop();
    }

    if (getOrCreateRssShuffleManager() != null) {
      getOrCreateRssShuffleManager().stop();
    }

    coordinatorClients.forEach(CoordinatorClient::close);
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return getOrCreateEssShuffleManager().shuffleBlockResolver();
  }
}
