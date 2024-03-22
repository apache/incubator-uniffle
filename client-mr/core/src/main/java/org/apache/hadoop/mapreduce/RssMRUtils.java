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

package org.apache.hadoop.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;

public class RssMRUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssMRUtils.class);
  private static final BlockIdLayout LAYOUT = BlockIdLayout.DEFAULT;
  private static final int MAX_ATTEMPT_LENGTH = 4;
  private static final int MAX_ATTEMPT_ID = (1 << MAX_ATTEMPT_LENGTH) - 1;
  private static final int MAX_TASK_LENGTH = LAYOUT.taskAttemptIdBits - MAX_ATTEMPT_LENGTH;
  private static final int MAX_TASK_ID = (1 << MAX_TASK_LENGTH) - 1;

  // Class TaskAttemptId have two field id and mapId. MR have a trick logic, taskAttemptId will
  // increase 1000 * (appAttemptId - 1), so we will decrease it.
  public static int createRssTaskAttemptId(TaskAttemptID taskAttemptID, int appAttemptId) {
    if (appAttemptId < 1) {
      throw new RssException("appAttemptId " + appAttemptId + " is wrong");
    }
    int lowBytes = taskAttemptID.getId() - (appAttemptId - 1) * 1000;
    if (lowBytes > MAX_ATTEMPT_ID || lowBytes < 0) {
      throw new RssException(
          "TaskAttempt " + taskAttemptID + " low bytes " + lowBytes + " exceed " + MAX_ATTEMPT_ID);
    }
    int highBytes = taskAttemptID.getTaskID().getId();
    if (highBytes > MAX_TASK_ID || highBytes < 0) {
      throw new RssException(
          "TaskAttempt " + taskAttemptID + " high bytes " + highBytes + " exceed " + MAX_TASK_ID);
    }
    return (highBytes << (MAX_ATTEMPT_LENGTH)) + lowBytes;
  }

  public static TaskAttemptID createMRTaskAttemptId(
      JobID jobID, TaskType taskType, int rssTaskAttemptId, int appAttemptId) {
    if (appAttemptId < 1) {
      throw new RssException("appAttemptId " + appAttemptId + " is wrong");
    }
    int task = rssTaskAttemptId >> MAX_ATTEMPT_LENGTH;
    int attempt = rssTaskAttemptId & MAX_ATTEMPT_ID;
    TaskID taskID = new TaskID(jobID, taskType, task);
    int id = attempt + 1000 * (appAttemptId - 1);
    return new TaskAttemptID(taskID, id);
  }

  public static ShuffleWriteClient createShuffleClient(JobConf jobConf) {
    int heartBeatThreadNum =
        jobConf.getInt(
            RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
            RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax =
        jobConf.getInt(
            RssMRConfig.RSS_CLIENT_RETRY_MAX, RssMRConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax =
        jobConf.getLong(
            RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String clientType =
        jobConf.get(RssMRConfig.RSS_CLIENT_TYPE, RssMRConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int replicaWrite =
        jobConf.getInt(
            RssMRConfig.RSS_DATA_REPLICA_WRITE, RssMRConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead =
        jobConf.getInt(
            RssMRConfig.RSS_DATA_REPLICA_READ, RssMRConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    int replica =
        jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA, RssMRConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    boolean replicaSkipEnabled =
        jobConf.getBoolean(
            RssMRConfig.RSS_DATA_REPLICA_SKIP_ENABLED,
            RssMRConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);
    int dataTransferPoolSize =
        jobConf.getInt(
            RssMRConfig.RSS_DATA_TRANSFER_POOL_SIZE,
            RssMRConfig.RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE);
    int dataCommitPoolSize =
        jobConf.getInt(
            RssMRConfig.RSS_DATA_COMMIT_POOL_SIZE,
            RssMRConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);
    ShuffleWriteClient client =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(replica)
                    .replicaWrite(replicaWrite)
                    .replicaRead(replicaRead)
                    .replicaSkipEnabled(replicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize)
                    .rssConf(RssMRConfig.toRssConf(jobConf)));
    return client;
  }

  public static Set<ShuffleServerInfo> getAssignedServers(Configuration jobConf, int reduceID) {
    String servers = jobConf.get(RssMRConfig.RSS_ASSIGNMENT_PREFIX + String.valueOf(reduceID));
    String[] splitServers = servers.split(",");
    Set<ShuffleServerInfo> assignServers = Sets.newHashSet();
    buildAssignServers(reduceID, splitServers, assignServers);
    return assignServers;
  }

  public static ApplicationAttemptId getApplicationAttemptId() {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    return containerId.getApplicationAttemptId();
  }

  public static void applyClientConf(Configuration jobConf, JobConf mrJobConf) {

    if (jobConf == null) {
      LOG.warn("Job conf is null");
      return;
    }

    if (mrJobConf == null) {
      LOG.warn("Empty conf items");
      return;
    }

    Iterator<Map.Entry<String, String>> iterator = mrJobConf.iterator();
    Map<String, String> confItems = new HashMap<>();

    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      String key = entry.getKey();
      if (!key.startsWith(RssMRConfig.MR_RSS_CONFIG_PREFIX)) {
        continue;
      }
      confItems.put(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String mrConfKey = kv.getKey();
      String mrConfVal = kv.getValue();
      if (StringUtils.isEmpty(jobConf.get(mrConfKey, ""))) {
        LOG.warn("Use conf client conf {} = {}", mrConfKey, mrConfVal);
        jobConf.set(mrConfKey, mrConfVal);
      }
    }
  }

  public static void applyDynamicClientConf(Configuration jobConf, Map<String, String> confItems) {
    if (jobConf == null) {
      LOG.warn("Job conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String mrConfKey = kv.getKey();
      if (!mrConfKey.startsWith(RssMRConfig.MR_CONFIG_PREFIX)) {
        mrConfKey = RssMRConfig.MR_CONFIG_PREFIX + mrConfKey;
      }
      String mrConfVal = kv.getValue();
      if (StringUtils.isEmpty(jobConf.get(mrConfKey, ""))
          || RssMRConfig.RSS_MANDATORY_CLUSTER_CONF.contains(mrConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", mrConfKey, mrConfVal);
        jobConf.set(mrConfKey, mrConfVal);
      }
    }
  }

  public static int getInt(Configuration rssJobConf, String key, int defaultValue) {
    return rssJobConf.getInt(key, defaultValue);
  }

  public static long getLong(Configuration rssJobConf, String key, long defaultValue) {
    return rssJobConf.getLong(key, defaultValue);
  }

  public static boolean getBoolean(Configuration rssJobConf, String key, boolean defaultValue) {
    return rssJobConf.getBoolean(key, defaultValue);
  }

  public static double getDouble(Configuration rssJobConf, String key, double defaultValue) {
    return rssJobConf.getDouble(key, defaultValue);
  }

  public static String getString(Configuration rssJobConf, String key) {
    return rssJobConf.get(key, "");
  }

  public static String getString(Configuration rssJobConf, String key, String defaultValue) {
    return rssJobConf.get(key, defaultValue);
  }

  public static long getBlockId(int partitionId, int taskAttemptId, int nextSeqNo) {
    return LAYOUT.getBlockId(nextSeqNo, partitionId, taskAttemptId);
  }

  public static long getTaskAttemptId(long blockId) {
    return LAYOUT.getTaskAttemptId(blockId);
  }

  public static int estimateTaskConcurrency(JobConf jobConf) {
    double dynamicFactor =
        jobConf.getDouble(
            RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR,
            RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE);
    double slowStart =
        jobConf.getDouble(Constants.MR_SLOW_START, Constants.MR_SLOW_START_DEFAULT_VALUE);
    int mapNum = jobConf.getNumMapTasks();
    int reduceNum = jobConf.getNumReduceTasks();
    int mapLimit = jobConf.getInt(Constants.MR_MAP_LIMIT, Constants.MR_MAP_LIMIT_DEFAULT_VALUE);
    int reduceLimit =
        jobConf.getInt(Constants.MR_REDUCE_LIMIT, Constants.MR_REDUCE_LIMIT_DEFAULT_VALUE);

    int estimateMapNum = mapLimit > 0 ? Math.min(mapNum, mapLimit) : mapNum;
    int estimateReduceNum = reduceLimit > 0 ? Math.min(reduceNum, reduceLimit) : reduceNum;
    if (slowStart == 1) {
      return (int) (Math.max(estimateMapNum, estimateReduceNum) * dynamicFactor);
    } else {
      return (int) (((1 - slowStart) * estimateMapNum + estimateReduceNum) * dynamicFactor);
    }
  }

  public static int getRequiredShuffleServerNumber(JobConf jobConf) {
    int requiredShuffleServerNumber =
        jobConf.getInt(
            RssMRConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
            RssMRConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE);
    boolean enabledEstimateServer =
        jobConf.getBoolean(
            RssMRConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED,
            RssMRConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE);
    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
      return requiredShuffleServerNumber;
    }
    int taskConcurrency = estimateTaskConcurrency(jobConf);
    int taskConcurrencyPerServer =
        jobConf.getInt(
            RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER,
            RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE);
    return (int) Math.ceil(taskConcurrency * 1.0 / taskConcurrencyPerServer);
  }

  public static void validateRssClientConf(Configuration rssJobConf) {
    int retryMax =
        getInt(
            rssJobConf,
            RssMRConfig.RSS_CLIENT_RETRY_MAX,
            RssMRConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax =
        getLong(
            rssJobConf,
            RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    long sendCheckTimeout =
        getLong(
            rssJobConf,
            RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
            RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    if (retryIntervalMax * retryMax > sendCheckTimeout) {
      throw new IllegalArgumentException(
          String.format(
              "%s(%s) * %s(%s) should not bigger than %s(%s)",
              RssMRConfig.RSS_CLIENT_RETRY_MAX,
              retryMax,
              RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
              retryIntervalMax,
              RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
              sendCheckTimeout));
    }
  }

  public static void buildAssignServers(
      int reduceId, String[] splitServers, Collection<ShuffleServerInfo> assignServers) {
    for (String splitServer : splitServers) {
      String[] serverInfo = splitServer.split(":");
      if (serverInfo.length != 2 && serverInfo.length != 3) {
        throw new RssException("partition " + reduceId + " server info isn't right");
      }
      ShuffleServerInfo server;
      if (serverInfo.length == 2) {
        server =
            new ShuffleServerInfo(
                StringUtils.join(serverInfo, "-"), serverInfo[0], Integer.parseInt(serverInfo[1]));
      } else {
        server =
            new ShuffleServerInfo(
                StringUtils.join(serverInfo, "-"),
                serverInfo[0],
                Integer.parseInt(serverInfo[1]),
                Integer.parseInt(serverInfo[2]));
      }
      assignServers.add(server);
    }
  }
}
