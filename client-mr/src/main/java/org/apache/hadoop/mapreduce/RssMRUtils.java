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

import org.apache.hadoop.mapred.JobConf;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;

public class RssMRUtils {

  // Class TaskAttemptId have two field id and mapId, mapId is 4 low byte,
  // id is high 4 byte.
  public static long convertTaskAttemptIdToLong(TaskAttemptID taskAttemptID) {
    long lowBytes = taskAttemptID.getTaskID().getId();
    long highBytes = (long)taskAttemptID.getId() << 32;
    return highBytes + lowBytes;
  }

  public static TaskAttemptID createMRTaskAttemptId(JobID jobID, TaskType taskType, long rssTaskAttemptId) {
    TaskID taskID = new TaskID(jobID,taskType, (int)rssTaskAttemptId);
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

}
