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

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezClientConf;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.common.sort.buffer.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteUnit;
import org.apache.uniffle.storage.util.StorageType;

/** {@link RssUnSorter} is an {@link ExternalSorter} */
public class RssUnSorter extends ExternalSorter {

  private static final Logger LOG = LoggerFactory.getLogger(RssUnSorter.class);
  private WriteBufferManager bufferManager;
  private Set<Long> successBlockIds = Sets.newConcurrentHashSet();
  private Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
  private int[] numRecordsPerPartition;

  /** Initialization */
  public RssUnSorter(
      TezTaskAttemptID tezTaskAttemptID,
      OutputContext outputContext,
      Configuration conf,
      int numMaps,
      int numOutputs,
      long initialMemoryAvailable,
      int shuffleId,
      ApplicationAttemptId applicationAttemptId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers)
      throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);
    this.numRecordsPerPartition = new int[numOutputs];
    TezClientConf tezClientConf = new TezClientConf(conf);
    long sortmb = tezClientConf.get(TezClientConf.RSS_RUNTIME_IO_SORT_MB);
    LOG.info("conf.sortmb is {}", sortmb);
    sortmb = this.availableMemoryMb;
    LOG.info("sortmb, availableMemoryMb is {}, {}", sortmb, availableMemoryMb);
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + TezClientConf.RSS_RUNTIME_IO_SORT_MB.key() + "\": " + sortmb);
    }
    double sortThreshold = tezClientConf.get(TezClientConf.RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD);
    final long taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptID);
    final long maxSegmentSize = tezClientConf.get(TezClientConf.RSS_CLIENT_MAX_BUFFER_SIZE);
    final long maxBufferSize = tezClientConf.get(TezClientConf.RSS_WRITER_BUFFER_SIZE);
    final double memoryThreshold = tezClientConf.get(TezClientConf.RSS_CLIENT_MEMORY_THRESHOLD);
    final int sendThreadNum = tezClientConf.get(TezClientConf.RSS_CLIENT_SEND_THREAD_NUM);
    final double sendThreshold = tezClientConf.get(TezClientConf.RSS_CLIENT_SEND_THRESHOLD);
    final int batch = tezClientConf.get(TezClientConf.RSS_CLIENT_BATCH_TRIGGER_NUM);
    String storageType = tezClientConf.getString(TezClientConf.RSS_STORAGE_TYPE, "MEMORY");
    if (StringUtils.isEmpty(storageType)) {
      throw new RssException("storage type mustn't be empty");
    }
    long sendCheckInterval = tezClientConf.get(TezClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    long sendCheckTimeout = tezClientConf.get(TezClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    int bitmapSplitNum = tezClientConf.get(TezClientConf.RSS_CLIENT_BITMAP_NUM);

    LOG.info("sortmb is {}", sortmb);
    LOG.info("sortThreshold is {}", sortThreshold);
    LOG.info("taskAttemptId is {}", taskAttemptId);
    LOG.info("maxSegmentSize is {}", maxSegmentSize);
    LOG.info("maxBufferSize is {}", maxBufferSize);
    LOG.info("memoryThreshold is {}", memoryThreshold);
    LOG.info("sendThreadNum is {}", sendThreadNum);
    LOG.info("sendThreshold is {}", sendThreshold);
    LOG.info("batch is {}", batch);
    LOG.info("storageType is {}", storageType);
    LOG.info("sendCheckInterval is {}", sendCheckInterval);
    LOG.info("sendCheckTimeout is {}", sendCheckTimeout);
    LOG.info("bitmapSplitNum is {}", bitmapSplitNum);

    LOG.info("applicationAttemptId is {}", applicationAttemptId.toString());

    bufferManager =
        new WriteBufferManager(
            tezTaskAttemptID,
            (long) (ByteUnit.MiB.toBytes(sortmb) * sortThreshold),
            applicationAttemptId.toString(),
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            RssTezUtils.createShuffleClient(tezClientConf),
            comparator,
            maxSegmentSize,
            keySerializer,
            valSerializer,
            maxBufferSize,
            memoryThreshold,
            sendThreadNum,
            sendThreshold,
            batch,
            new RssConf(),
            partitionToServers,
            numMaps,
            isMemoryShuffleEnabled(storageType),
            sendCheckInterval,
            sendCheckTimeout,
            bitmapSplitNum,
            shuffleId,
            false,
            mapOutputByteCounter,
            mapOutputRecordCounter);
    LOG.info("Initialized WriteBufferManager.");
  }

  @Override
  public void flush() throws IOException {
    bufferManager.waitSendFinished();
  }

  @Override
  public final void close() throws IOException {
    super.close();
    bufferManager.freeAllResources();
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    try {
      collect(key, value, partitioner.getPartition(key, value, partitions));
    } catch (InterruptedException e) {
      throw new RssException(e);
    }
  }

  synchronized void collect(Object key, Object value, final int partition)
      throws IOException, InterruptedException {
    if (key.getClass() != keyClass) {
      throw new IOException(
          "Type mismatch in key from map: expected "
              + keyClass.getName()
              + ", received "
              + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException(
          "Type mismatch in value from map: expected "
              + valClass.getName()
              + ", received "
              + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" + partition + ")");
    }
    bufferManager.addRecord(partition, key, value);
    numRecordsPerPartition[partition]++;
  }

  public int[] getNumRecordsPerPartition() {
    return numRecordsPerPartition;
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.withMemory(StorageType.valueOf(storageType));
  }
}
