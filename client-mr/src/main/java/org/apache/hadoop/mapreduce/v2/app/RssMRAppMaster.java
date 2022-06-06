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

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.RemoteStorageInfo;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;

public class RssMRAppMaster {

  private static final Logger LOG = LoggerFactory.getLogger(RssMRAppMaster.class);

  public static void main(String[] args) {


    JobConf conf = new JobConf(new Path(MRJobConfig.JOB_CONF_FILE));
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    if (numReduceTasks > 0) {
      String coordinators = conf.get(RssMRConfig.RSS_COORDINATOR_QUORUM);

      ShuffleWriteClient client = RssMRUtils.createShuffleClient(conf);

      LOG.info("Registering coordinators {}", coordinators);
      client.registerCoordinators(coordinators);

      ApplicationAttemptId applicationAttemptId = RssMRUtils.getApplicationAttemptId();
      String appId = applicationAttemptId.toString();
      ShuffleAssignmentsInfo response = client.getShuffleAssignments(
          appId, 0, numReduceTasks,
          1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));

      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = response.getServerToPartitionRanges();
      final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = Executors.defaultThreadFactory().newThread(r);
              t.setDaemon(true);
              return t;
            }
          }
      );
      if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
        return;
      }

      long heartbeatInterval = conf.getLong(RssMRConfig.RSS_HEARTBEAT_INTERVAL,
          RssMRConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
      long heartbeatTimeout = conf.getLong(RssMRConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
      scheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              client.sendAppHeartbeat(appId, heartbeatTimeout);
              LOG.info("Finish send heartbeat to coordinator and servers");
            } catch (Exception e) {
              LOG.warn("Fail to send heartbeat to coordinator and servers", e);
            }
          },
          heartbeatInterval / 2,
          heartbeatInterval,
          TimeUnit.MILLISECONDS);

      JobConf extraConf = new JobConf();
      extraConf.clear();
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
        extraConf.set(RssMRConfig.RSS_ASSIGNMENT_PREFIX + entry.getKey(), StringUtils.join(servers, ","));
      });

      // get remote storage from coordinator if necessary
      boolean dynamicConfEnabled = conf.getBoolean(RssMRConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
          RssMRConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

      // fetch client conf and apply them if necessary
      if (dynamicConfEnabled) {
        Map<String, String> clusterClientConf = client.fetchClientConf(
            conf.getInt(RssMRConfig.RSS_ACCESS_TIMEOUT_MS,
                RssMRConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE));
        RssMRUtils.applyDynamicClientConf(extraConf, clusterClientConf);
      }

      String storageType = conf.get(RssMRConfig.RSS_STORAGE_TYPE);
      RemoteStorageInfo defaultRemoteStorage =
          new RemoteStorageInfo(conf.get(RssMRConfig.RSS_REMOTE_STORAGE_PATH, ""));
      RemoteStorageInfo remoteStorage = ClientUtils.fetchRemoteStorage(
        appId, defaultRemoteStorage, dynamicConfEnabled, storageType, client);
      // set the remote storage with actual value
      extraConf.set(RssMRConfig.RSS_REMOTE_STORAGE_PATH, remoteStorage.getPath());
      extraConf.set(RssMRConfig.RSS_REMOTE_STORAGE_CONF, remoteStorage.getConfString());

      LOG.info("Start to register shuffle");
      long start = System.currentTimeMillis();
      serverToPartitionRanges.entrySet().forEach(entry -> {
        client.registerShuffle(
            entry.getKey(), appId, 0, entry.getValue(), remoteStorage);
      });
      LOG.info("Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");

      writeExtraConf(conf, extraConf);

      // close slow start
      conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);
      LOG.warn("close slow start, because RSS does not support it yet");

      // MapReduce don't set setKeepContainersAcrossApplicationAttempts in AppContext, there will be no container
      // to be shared between attempts. Rss don't support shared container between attempts.
      conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false);
      LOG.warn("close recovery enable, because RSS doesn't support it yet");

      String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
      if (jobDirStr == null) {
        throw new RuntimeException("jobDir is empty");
      }
      Path jobConfFile = new Path(jobDirStr, MRJobConfig.JOB_CONF_FILE);

      updateConf(conf, jobConfFile);
    }
    // remove org.apache.hadoop.mapreduce.v2.app.MRAppMaster
    ArrayUtils.remove(args, 0);
    MRAppMaster.main(args);
  }

  static void writeExtraConf(JobConf conf, JobConf extraConf) {
    try {
      FileSystem fs = new Cluster(conf).getFileSystem();
      String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
      Path assignmentFile = new Path(jobDirStr, RssMRConfig.RSS_CONF_FILE);

      try (FSDataOutputStream out =
               FileSystem.create(fs, assignmentFile,
                   new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
        extraConf.writeXml(out);
      }
      FileStatus status = fs.getFileStatus(assignmentFile);
      long currentTs = status.getModificationTime();
      String uri = fs.getUri() + Path.SEPARATOR + assignmentFile.toUri();
      String files = conf.get(MRJobConfig.CACHE_FILES);
      conf.set(MRJobConfig.CACHE_FILES, files == null ? uri : uri + "," + files);
      String ts = conf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS);
      conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS,
          ts == null ? String.valueOf(currentTs) : currentTs + "," + ts);
      String vis = conf.get(MRJobConfig.CACHE_FILE_VISIBILITIES);
      conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, vis == null ? "false" : "false" + "," + vis);
      long size = status.getLen();
      String sizes = conf.get(MRJobConfig.CACHE_FILES_SIZES);
      conf.set(MRJobConfig.CACHE_FILES_SIZES, sizes == null ? String.valueOf(size) : size + "," + sizes);
    } catch (Exception e) {
      LOG.error("Upload extra conf exception", e);
      throw new RuntimeException("Upload extra conf exception ", e);
    }
  }

  // After we modify some configurations, we should update configuration for Application
  // So we update the modify the configuration in the HDFS and local disk. It's a little
  // tricky to delete the local configuration. But we hope guarantee the integrity for file.
  // We choose to delete it and override with the new configuration in the HDFS.
  static void updateConf(JobConf conf, Path jobConfFile) {
    try {
      File newFile = new File(MRJobConfig.JOB_CONF_FILE + ".bak");
      try (FileOutputStream out = new FileOutputStream(newFile)) {
          conf.writeXml(out);
      }
      File file = new File(MRJobConfig.JOB_CONF_FILE);
      file.delete();
      newFile.renameTo(file);
    } catch (Exception e) {
      LOG.error("Modify job conf exception", e);
      throw new RuntimeException("Modify job conf exception ", e);
    }
  }
}
