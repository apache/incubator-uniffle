/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;

public class RssMRUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssMRUtils.class);

  // Class TaskAttemptId have two field id and mapId, mapId is 4 low byte,
  // id is high 4 byte.
  public static long convertTaskAttemptIdToLong(TaskAttemptID taskAttemptID) {
    long lowBytes = taskAttemptID.getTaskID().getId();
    long highBytes = (long)taskAttemptID.getId() << 32;
    return highBytes + lowBytes;
  }

  public static TaskAttemptID createMRTaskAttemptId(JobID jobID, TaskType taskType,
                                                    long rssTaskAttemptId) {
    TaskID taskID = new TaskID(jobID, taskType, (int)rssTaskAttemptId);
    return new TaskAttemptID(taskID, (int)(rssTaskAttemptId >> 32));
  }

  public static ShuffleWriteClient createShuffleClient(JobConf jobConf) {
    int heartBeatThreadNum = jobConf.getInt(RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
        RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax = jobConf.getInt(RssMRConfig.RSS_CLIENT_RETRY_MAX,
        RssMRConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = jobConf.getLong(RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String clientType = jobConf.get(RssMRConfig.RSS_CLIENT_TYPE,
        RssMRConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int replicaWrite = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA_WRITE,
        RssMRConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA_READ,
        RssMRConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    int replica = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA,
        RssMRConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    ShuffleWriteClient client = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax,
            heartBeatThreadNum, replica, replicaWrite, replicaRead);
    return client;
  }

  public static Set<ShuffleServerInfo> getAssignedServers(JobConf jobConf, int reduceID) {
    String servers = jobConf.get(RssMRConfig.RSS_ASSIGNMENT_PREFIX
      + String.valueOf(reduceID));
    String[] splitServers = servers.split(",");
    Set<ShuffleServerInfo> assignServers = Sets.newHashSet();
    for (String splitServer : splitServers) {
      String[] serverInfo = splitServer.split(":");
      if (serverInfo.length != 2) {
        throw new RssException("partition " + reduceID + " server info isn't right");
      }
      ShuffleServerInfo sever = new ShuffleServerInfo(StringUtils.join(serverInfo, "-"),
        serverInfo[0], Integer.parseInt(serverInfo[1]));
      assignServers.add(sever);
    }
    return assignServers;
  }

  public static ApplicationAttemptId getApplicationAttemptId() {
    String containerIdStr =
      System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    return containerId.getApplicationAttemptId();
  }

  public static void applyDynamicClientConf(JobConf jobConf, Map<String, String> confItems) {
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
      if (!mrConfKey.startsWith(RssMRConfig.MR_RSS_CONFIG_PREFIX)) {
        mrConfKey = RssMRConfig.MR_RSS_CONFIG_PREFIX + mrConfKey;
      }
      String mrConfVal = kv.getValue();
      if (StringUtils.isEmpty(jobConf.get(mrConfKey, ""))
          || RssMRConfig.RSS_MANDATORY_CLUSTER_CONF.contains(mrConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", mrConfKey, mrConfVal);
        jobConf.set(mrConfKey, mrConfVal);
      }
    }
  }
}
