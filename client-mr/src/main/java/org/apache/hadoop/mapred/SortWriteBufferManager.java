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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.RssShuffleUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.ChecksumUtils;

public class SortWriteBufferManager<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(SortWriteBufferManager.class);

  private final long maxMemSize;
  private final Map<Integer, SortWriteBuffer<K, V>> buffers = Maps.newConcurrentMap();
  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private final Counters.Counter mapOutputByteCounter;
  private final Counters.Counter mapOutputRecordCounter;
  private long uncompressedDataLen = 0;
  private long compressTime = 0;
  private final long taskAttemptId;
  private final AtomicLong memoryUsedSize = new AtomicLong(0);
  private final int batch;
  private final AtomicLong inSendListBytes = new AtomicLong(0);
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private double memoryThreshold;
  private final ReentrantLock memoryLock = new ReentrantLock();
  private final Condition full = memoryLock.newCondition();
  private final Serializer<K> keySerializer;
  private final Serializer<V> valSerializer;
  private final RawComparator<K> comparator;
  private final Set<Long> successBlockIds;
  private final Set<Long> failedBlockIds;
  private final List<SortWriteBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();
  private final String appId;
  private final ShuffleWriteClient shuffleWriteClient;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final Set<Long> allBlockIds = Sets.newConcurrentHashSet();
  private final int bitmapSplitNum;
  private final Map<Integer, List<Long>> partitionToBlocks = Maps.newConcurrentMap();
  private long maxSegmentSize;
  private final boolean isMemoryShuffleEnabled;
  private final int numMaps;
  private long copyTime = 0;
  private long sortTime = 0;
  private final ExecutorService sendExecutorService = Executors.newFixedThreadPool(
      5,
      new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("send-thread-%d")
      .build());

  public SortWriteBufferManager(
      long maxMemSize,
      long taskAttemptId,
      int batch,
      Serializer<K> keySerializer,
      Serializer<V> valSerializer,
      RawComparator<K> comparator,
      double memoryThreshold,
      String appId,
      ShuffleWriteClient shuffleWriteClient,
      long sendCheckInterval,
      long sendCheckTimeout,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Set<Long> successBlockIds,
      Set<Long> failedBlockIds,
      Counters.Counter mapOutputByteCounter,
      Counters.Counter mapOutputRecordCounter,
      int bitmapSplitNum,
      long maxSegmentSize,
      int numMaps,
      boolean isMemoryShuffleEnabled) {
    this.maxMemSize = maxMemSize;
    this.taskAttemptId = taskAttemptId;
    this.batch = batch;
    this.keySerializer = keySerializer;
    this.valSerializer = valSerializer;
    this.comparator = comparator;
    this.memoryThreshold = memoryThreshold;
    this.appId = appId;
    this.shuffleWriteClient = shuffleWriteClient;
    this.sendCheckInterval = sendCheckInterval;
    this.sendCheckTimeout = sendCheckTimeout;
    this.partitionToServers = partitionToServers;
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
    this.bitmapSplitNum = bitmapSplitNum;
    this.maxSegmentSize = maxSegmentSize;
    this.numMaps = numMaps;
    this.isMemoryShuffleEnabled = isMemoryShuffleEnabled;
  }

  // todo: Single Buffer should also have its size limit
  public void addRecord(int partitionId, K key, V value) throws IOException,InterruptedException {
    memoryLock.lock();
    try {
      while (memoryUsedSize.get() > maxMemSize) {
        full.await();
      }
    } finally {
      memoryLock.unlock();;
    }

    if (!buffers.containsKey(partitionId)) {
      SortWriteBuffer<K, V> sortWriterBuffer = new SortWriteBuffer(
          partitionId, comparator, maxSegmentSize, keySerializer, valSerializer);
      buffers.putIfAbsent(partitionId, sortWriterBuffer);
      waitSendBuffers.add(sortWriterBuffer);
    }
    SortWriteBuffer<K, V> buffer = buffers.get(partitionId);
    long length = buffer.addRecord(key, value);
    if (length > maxMemSize) {
      throw new RssException("record is too big");
    }
    memoryUsedSize.addAndGet(length);
    if (memoryUsedSize.get() > maxMemSize * memoryThreshold) {
      sendBuffersToServers();
    }
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(length);
  }

  // Only for test
  void sendBuffersToServers() {
    waitSendBuffers.sort(new Comparator<SortWriteBuffer<K, V>>() {
      @Override
      public int compare(SortWriteBuffer<K, V> o1, SortWriteBuffer<K, V> o2) {
        return o1.getDataLength() - o2.getDataLength();
      }
    });
    int sendSize = batch;
    if (batch > waitSendBuffers.size()) {
      sendSize = waitSendBuffers.size();
    }
    List<SortWriteBuffer<K, V>> selectBuffers = Lists.newArrayList();
    Iterator<SortWriteBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    while (iterator.hasNext() && index < sendSize) {
      selectBuffers.add(iterator.next());
      iterator.remove();
      index++;
    }
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    long keyLength = 0;
    for (SortWriteBuffer buffer : selectBuffers) {
      keyLength += buffer.getTotalKeyLength();
      buffers.remove(buffer.getPartitionId());
      ShuffleBlockInfo block = createShuffleBlock(buffer);
      shuffleBlocks.add(block);
      allBlockIds.add(block.getBlockId());
      if (!partitionToBlocks.containsKey(block.getPartitionId())) {
        partitionToBlocks.putIfAbsent(block.getPartitionId(), Lists.newArrayList());
      }
      partitionToBlocks.get(block.getPartitionId()).add(block.getBlockId());
    }
    long finalKeyLength = keyLength;
    sendExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        long size = 0;
        try {
          for (ShuffleBlockInfo block : shuffleBlocks) {
             size += block.getFreeMemory();
          }
          inSendListBytes.addAndGet(size);
          SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(appId, shuffleBlocks);
          successBlockIds.addAll(result.getSuccessBlockIds());
          failedBlockIds.addAll(result.getFailedBlockIds());
        } catch (Throwable t) {
          LOG.warn("send shuffle data exception ", t);
        } finally {
          try {
            memoryLock.lock();
            memoryUsedSize.addAndGet(-size);
            memoryUsedSize.addAndGet(-finalKeyLength);
            inSendListBytes.addAndGet(-size);
            full.signalAll();
          } finally {
            memoryLock.unlock();
          }
        }
      }
    });
  }

  public void waitSendFinished() {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    long start = System.currentTimeMillis();
    long commitDuration = 0;
    if (!isMemoryShuffleEnabled) {
      long s = System.currentTimeMillis();
      sendCommit();
      commitDuration = System.currentTimeMillis() - s;
    }
    while (true) {
      // if failed when send data to shuffle server, mark task as failed
      if (failedBlockIds.size() > 0) {
        String errorMsg =
            "Send failed: failed because " + failedBlockIds.size()
                + " blocks can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }

      // remove blockIds which was sent successfully, if there has none left, all data are sent
      allBlockIds.removeAll(successBlockIds);
      if (allBlockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + allBlockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
            "Timeout: failed because " + allBlockIds.size()
                + " blocks can't be sent to shuffle server in " + sendCheckTimeout + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }

    start = System.currentTimeMillis();
    shuffleWriteClient.reportShuffleResult(partitionToServers, appId, 0,
        taskAttemptId, partitionToBlocks, bitmapSplitNum);
    LOG.info("Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
        taskAttemptId, bitmapSplitNum, (System.currentTimeMillis() - start));
    LOG.info("Task uncompressed data length {} compress time cost {} ms, commit time cost {} ms,"
            + " copy time cost {} ms, sort time cost {} ms",
        uncompressedDataLen, compressTime, commitDuration, copyTime, sortTime);
  }

  // transform records to shuffleBlock
  ShuffleBlockInfo createShuffleBlock(SortWriteBuffer wb) {
    byte[] data = wb.getData();
    copyTime += wb.getCopyTime();
    sortTime += wb.getSortTime();
    int partitionId = wb.getPartitionId();
    final int uncompressLength = data.length;
    long start = System.currentTimeMillis();
    final byte[] compressed = RssShuffleUtils.compressData(data);
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    compressTime += System.currentTimeMillis() - start;
    final long blockId = ClientUtils.getBlockId(partitionId, taskAttemptId, getNextSeqNo(partitionId));
    uncompressedDataLen += data.length;
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getDataLength());
    return new ShuffleBlockInfo(0, partitionId, blockId, compressed.length, crc32,
        compressed, partitionToServers.get(partitionId), uncompressLength, wb.getDataLength(), taskAttemptId);
  }

  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    for (List<ShuffleServerInfo> serverInfoLists : partitionToServers.values()) {
      for (ShuffleServerInfo serverInfo : serverInfoLists) {
        serverInfos.add(serverInfo);
      }
    }
    Future<Boolean> future = executor.submit(
        () -> shuffleWriteClient.sendCommit(serverInfos, appId, 0, numMaps));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
    while (!future.isDone()) {
      LOG.info("Wait commit to shuffle server for task[" + taskAttemptId + "] cost "
          + (System.currentTimeMillis() - start) + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      // check if commit/finish rpc is successful
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RuntimeException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  // it's run in single thread, and is not thread safe
  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, new Integer(0));
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void freeAllResources() {
    sendExecutorService.shutdownNow();
  }
}
