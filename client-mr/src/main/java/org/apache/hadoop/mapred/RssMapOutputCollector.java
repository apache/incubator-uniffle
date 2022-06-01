/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available
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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RssMRConfig;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.mapreduce.TaskCounter;

import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.ByteUnit;
import com.tencent.rss.storage.util.StorageType;

public class RssMapOutputCollector<K extends Object, V extends Object>
    implements MapOutputCollector<K, V> {

  private Task.TaskReporter reporter;
  private Class<K> keyClass;
  private Class<V> valClass;
  private Set<Long> successBlockIds = Sets.newConcurrentHashSet();
  private Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
  private int partitions;
  private SortWriteBufferManager bufferManager;

  @Override
  public void init(Context context) throws IOException, ClassNotFoundException {
    JobConf jobConf = context.getJobConf();
    reporter = context.getReporter();
    keyClass = (Class<K>)jobConf.getMapOutputKeyClass();
    valClass = (Class<V>)jobConf.getMapOutputValueClass();
    int sortmb = jobConf.getInt(JobContext.IO_SORT_MB, 100);
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
    }
    partitions = jobConf.getNumReduceTasks();
    MapTask mapTask = context.getMapTask();
    long taskAttemptId = RssMRUtils.convertTaskAttemptIdToLong(mapTask.getTaskID());
    int batch = jobConf.getInt(RssMRConfig.RSS_CLIENT_BATCH_TRIGGER_NUM,
        RssMRConfig.RSS_CLIENT_DEFAULT_BATCH_TRIGGER_NUM);
    RawComparator<K> comparator = jobConf.getOutputKeyComparator();
    double memoryThreshold = jobConf.getDouble(RssMRConfig.RSS_CLIENT_MEMORY_THRESHOLD,
        RssMRConfig.RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD);
    String appId = RssMRUtils.getApplicationAttemptId().toString();
    double sendThreshold = jobConf.getDouble(RssMRConfig.RSS_CLIENT_SEND_THRESHOLD,
        RssMRConfig.RSS_CLIENT_DEFAULT_SEND_THRESHOLD);

    long sendCheckInterval = jobConf.getLong(RssMRConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
        RssMRConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE);
    long sendCheckTimeout = jobConf.getLong(RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
        RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    int bitmapSplitNum = jobConf.getInt(RssMRConfig.RSS_CLIENT_BITMAP_NUM,
        RssMRConfig.RSS_CLIENT_DEFAULT_BITMAP_NUM);
    int numMaps = jobConf.getNumMapTasks();
    String storageType = jobConf.get(RssMRConfig.RSS_STORAGE_TYPE);
    if (StringUtils.isEmpty(storageType)) {
      throw new RssException("storage type mustn't be empty");
    }

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = createAssignmentMap(jobConf);

    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    long maxSegmentSize = jobConf.getLong(RssMRConfig.RSS_CLIENT_MAX_SEGMENT_SIZE,
        RssMRConfig.RSS_CLIENT_DEFAULT_MAX_SEGMENT_SIZE);
    int sendThreadNum = jobConf.getInt(RssMRConfig.RSS_CLIENT_SEND_THREAD_NUM,
        RssMRConfig.RSS_CLIENT_DEFAULT_SEND_THREAD_NUM);
    bufferManager = new SortWriteBufferManager(
        (long)ByteUnit.MiB.toBytes(sortmb),
        taskAttemptId,
        batch,
        serializationFactory.getSerializer(keyClass),
        serializationFactory.getSerializer(valClass),
        comparator,
        memoryThreshold,
        appId,
        RssMRUtils.createShuffleClient(jobConf),
        sendCheckInterval,
        sendCheckTimeout,
        partitionToServers,
        successBlockIds,
        failedBlockIds,
        reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES),
        reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS),
        bitmapSplitNum,
        maxSegmentSize,
        numMaps,
        isMemoryShuffleEnabled(storageType),
        sendThreadNum,
        sendThreshold);
  }

  private Map<Integer, List<ShuffleServerInfo>> createAssignmentMap(JobConf jobConf) {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    for (int i = 0; i < partitions; i++) {
      String servers = jobConf.get(RssMRConfig.RSS_ASSIGNMENT_PREFIX + i);
      if (StringUtils.isEmpty(servers)) {
        throw new RssException("assign partition " + i + " shouldn't be empty");
      }
      String[] splitServers = servers.split(",");
      List<ShuffleServerInfo> assignServers = Lists.newArrayList();
      for (String splitServer : splitServers) {
        String[] serverInfo = splitServer.split(":");
        if (serverInfo.length != 2) {
          throw new RssException("partition " + i + " server info isn't right");
        }
        ShuffleServerInfo sever = new ShuffleServerInfo(StringUtils.join(serverInfo, "-"),
            serverInfo[0], Integer.parseInt(serverInfo[1]));
        assignServers.add(sever);
      }
      partitionToServers.put(i, assignServers);
    }
    return partitionToServers;
  }

  @Override
  public void collect(K key, V value, int partition) throws IOException, InterruptedException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
          + keyClass.getName() + ", received "
          + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
          + valClass.getName() + ", received "
          + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " ("
          + partition + ")");
    }
    checkRssException();
    bufferManager.addRecord(partition, key, value);
  }

  private void checkRssException() {
    if (!failedBlockIds.isEmpty()) {
      throw new RssException("There are some blocks failed");
    }
  }

  @Override
  public void close() throws IOException, InterruptedException {
    reporter.progress();
    bufferManager.freeAllResources();
  }

  @Override
  public void flush() throws IOException, InterruptedException, ClassNotFoundException {
    reporter.progress();
    bufferManager.waitSendFinished();
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.MEMORY_LOCALFILE.name().equals(storageType)
        || StorageType.MEMORY_HDFS.name().equals(storageType)
        || StorageType.MEMORY_LOCALFILE_HDFS.name().equals(storageType);
  }
}
