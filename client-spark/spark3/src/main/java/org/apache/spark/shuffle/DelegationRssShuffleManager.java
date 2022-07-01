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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.request.RssAccessClusterRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAccessClusterResponse;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.Constants;

public class DelegationRssShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(DelegationRssShuffleManager.class);

  private final ShuffleManager delegate;
  private final List<CoordinatorClient> coordinatorClients;
  private final int accessTimeoutMs;
  private final SparkConf sparkConf;

  public DelegationRssShuffleManager(SparkConf sparkConf, boolean isDriver) throws Exception {
    this.sparkConf = sparkConf;
    accessTimeoutMs = sparkConf.getInt(
        RssSparkConfig.RSS_ACCESS_TIMEOUT_MS,
        RssSparkConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE);
    if (isDriver) {
      coordinatorClients = RssSparkShuffleUtils.createCoordinatorClients(sparkConf);
      delegate = createShuffleManagerInDriver();
    } else {
      coordinatorClients = Lists.newArrayList();
      delegate = createShuffleManagerInExecutor();
    }

    if (delegate == null) {
      throw new RssException("Fail to create shuffle manager!");
    }
  }

  private ShuffleManager createShuffleManagerInDriver() throws RssException {
    ShuffleManager shuffleManager;

    boolean canAccess = tryAccessCluster();
    if (canAccess) {
      try {
        shuffleManager = new RssShuffleManager(sparkConf, true);
        sparkConf.set(RssSparkConfig.RSS_ENABLED, "true");
        sparkConf.set("spark.shuffle.manager", RssShuffleManager.class.getCanonicalName());
        LOG.info("Use RssShuffleManager");
        return shuffleManager;
      } catch (Exception exception) {
        LOG.warn("Fail to create RssShuffleManager, fallback to SortShuffleManager {}", exception.getMessage());
      }
    }

    try {
      shuffleManager = RssSparkShuffleUtils.loadShuffleManager(Constants.SORT_SHUFFLE_MANAGER_NAME, sparkConf, true);
      sparkConf.set(RssSparkConfig.RSS_ENABLED, "false");
      sparkConf.set("spark.shuffle.manager", "sort");
      LOG.info("Use SortShuffleManager");
    } catch (Exception e) {
      throw new RssException(e.getMessage());
    }

    return shuffleManager;
  }

  private boolean tryAccessCluster() {
    String accessId = sparkConf.get(
        RssSparkConfig.RSS_ACCESS_ID, "").trim();
    if (StringUtils.isEmpty(accessId)) {
      LOG.warn("Access id key is empty");
      return false;
    }

    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      try {
        RssAccessClusterResponse response =
            coordinatorClient.accessCluster(new RssAccessClusterRequest(
                accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), accessTimeoutMs));
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          LOG.warn("Success to access cluster {} using {}", coordinatorClient.getDesc(), accessId);
          return true;
        } else if (response.getStatusCode() == ResponseStatusCode.ACCESS_DENIED) {
          LOG.warn("Request to access cluster {} is denied using {} for {}",
              coordinatorClient.getDesc(), accessId, response.getMessage());
          return false;
        } else {
          LOG.warn("Fail to reach cluster {} for {}", coordinatorClient.getDesc(), response.getMessage());
        }
      } catch (Exception e) {
        LOG.warn("Fail to access cluster {} using {} for {}",
            coordinatorClient.getDesc(), accessId, e.getMessage());
      }
    }

    return false;
  }

  private ShuffleManager createShuffleManagerInExecutor() throws RssException {
    ShuffleManager shuffleManager;
    // get useRSS from spark conf
    boolean useRSS = sparkConf.getBoolean(
        RssSparkConfig.RSS_ENABLED,
        RssSparkConfig.RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE);
    if (useRSS) {
      // Executor will not do any fallback
      shuffleManager = new RssShuffleManager(sparkConf, false);
      LOG.info("Use RssShuffleManager");
    } else {
      try {
        shuffleManager = RssSparkShuffleUtils.loadShuffleManager(
            Constants.SORT_SHUFFLE_MANAGER_NAME, sparkConf, false);
        LOG.info("Use SortShuffleManager");
      } catch (Exception e) {
        throw new RssException(e.getMessage());
      }
    }
    return shuffleManager;
  }

  public ShuffleManager getDelegate() {
    return delegate;
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, ShuffleDependency<K, V, C> dependency) {
    return delegate.registerShuffle(shuffleId, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle,
      long mapId,
      TaskContext context,
      ShuffleWriteMetricsReporter metrics) {
    return delegate.getWriter(handle, mapId, context, metrics);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return delegate.getReader(handle,
      startPartition, endPartition, context, metrics);
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
      reader = (ShuffleReader<K, C>)delegate.getClass().getDeclaredMethod(
        "getReader",
        ShuffleHandle.class,
        int.class,
        int.class,
        int.class,
        int.class,
        TaskContext.class,
        ShuffleReadMetricsReporter.class).invoke(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
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
      reader = (ShuffleReader<K, C>)delegate.getClass().getDeclaredMethod(
        "getReaderForRange",
        ShuffleHandle.class,
        int.class,
        int.class,
        int.class,
        int.class,
        TaskContext.class,
        ShuffleReadMetricsReporter.class).invoke(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return reader;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return delegate.unregisterShuffle(shuffleId);
  }

  @Override
  public void stop() {
    delegate.stop();
    coordinatorClients.forEach(CoordinatorClient::close);
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return delegate.shuffleBlockResolver();
  }
}
