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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RssMRConfig;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;

public class RssMRAppMaster {

  private static final Logger LOG = LoggerFactory.getLogger(RssMRAppMaster.class);

  public static void main(String[] args) {

    JobConf conf = new JobConf(new YarnConfiguration());
    conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    String clientType = conf.get(RssMRConfig.RSS_CLIENT_TYPE, RssMRConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int heartBeatThreadNum = conf.getInt(RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
        RssMRConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax = conf.getInt(RssMRConfig.RSS_CLIENT_RETRY_MAX, RssMRConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = conf.getLong(RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssMRConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String coordinators = conf.get(RssMRConfig.RSS_COORDINATOR_QUORUM);

    int replica = conf.getInt(RssMRConfig.RSS_DATA_REPLICA, RssMRConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    int replicaWrite = conf.getInt(RssMRConfig.RSS_DATA_REPLICA_WRITE,
        RssMRConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead = conf.getInt(RssMRConfig.RSS_DATA_REPLICA_READ,
        RssMRConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    ShuffleWriteClient client = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax,
            heartBeatThreadNum, replica, replicaWrite, replicaRead);

    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);

    ApplicationAttemptId applicationAttemptId = RssMRUtils.getApplicationAttemptId();
    ShuffleAssignmentsInfo response = client.getShuffleAssignments(
        applicationAttemptId.toString(), 0, numReduceTasks,
        1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = response.getServerToPartitionRanges();
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }

    long heartbeatInterval = conf.getLong(RssMRConfig.RSS_HEARTBEAT_INTERVAL,
        RssMRConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    long heartbeatTimeout = conf.getLong(RssMRConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            client.sendAppHeartbeat(applicationAttemptId.toString(), heartbeatTimeout);
            LOG.info("Finish send heartbeat to coordinator and servers");
          } catch (Exception e) {
            LOG.warn("Fail to send heartbeat to coordinator and servers", e);
          }
        },
        heartbeatInterval / 2,
        heartbeatInterval,
        TimeUnit.MILLISECONDS);

    LOG.info("Start to register shuffle");
    long start = System.currentTimeMillis();
    serverToPartitionRanges.entrySet().forEach(entry -> {
      client.registerShuffle(
          entry.getKey(), applicationAttemptId.toString(), 0, entry.getValue());
    });
    LOG.info("Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");

    // write shuffle worker assignments to submit work directory
    // format is as below:
    // mapreduce.rss.assignment.partition.1:server1,server2
    // mapreduce.rss.assignment.partition.2:server3,server4
    // ...

    response.getPartitionToServers().entrySet().forEach(entry -> {
      List<String> servers = Lists.newArrayList();
      for (ShuffleServerInfo server : entry.getValue()) {
        servers.add(server.getHost() + ":" + server.getPort());
      }
      conf.set(RssMRConfig.RSS_ASSIGNMENT_PREFIX + entry.getKey(), StringUtils.join(servers, ","));
    });

    // close slow start
    if (conf.getFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.05f) != 1) {
      conf.set(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, "1");
      LOG.warn("close slow start, because RSS does not support it yet");
    }

    String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
    if (jobDirStr == null) {
      throw new RuntimeException("jobDir is empty");
    }
    Path jobConfFile = new Path(jobDirStr, MRJobConfig.JOB_CONF_FILE);
    try {
      FileSystem fs = new Cluster(conf).getFileSystem();
      fs.delete(jobConfFile, true);
      try (FSDataOutputStream out =
             FileSystem.create(fs, jobConfFile,
                 new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
          conf.writeXml(out);
      }
    } catch (Exception e) {
      LOG.error("Modify job conf exception", e);
      throw new RuntimeException("Modify job conf exception ", e);
    }
    // remove org.apache.hadoop.mapreduce.v2.app.MRAppMaster
    ArrayUtils.remove(args, 0);
    MRAppMaster.main(args);
    scheduledExecutorService.shutdown();
  }
}
