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

package org.apache.spark.shuffle.writer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);
  private static final String DUMMY_HOST = "dummy_host";
  private static final int DUMMY_PORT = 99999;

  private final String appId;
  private final int shuffleId;
  private final WriteBufferManager bufferManager;
  private final String taskId;
  private final long taskAttemptId;
  private final int numMaps;
  private final ShuffleDependency<K, V, C> shuffleDependency;
  private final ShuffleWriteMetrics shuffleWriteMetrics;
  private final Partitioner partitioner;
  private final int numPartitions;
  private final RssShuffleManager shuffleManager;
  private final boolean shouldPartition;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final long sendSizeLimit;
  private final int blockNumPerTaskPartition;
  private final long blockNumPerBitmap;
  private final Map<Integer, Set<Long>> partitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final Set shuffleServersForData;
  private final long[] partitionLengths;


  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      WriteBufferManager bufferManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle rssHandle) {
    LOG.warn("RssShuffle start write taskAttemptId data" + taskAttemptId);
    this.shuffleManager = shuffleManager;
    this.appId = appId;
    this.bufferManager = bufferManager;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.shuffleDependency = rssHandle.getDependency();
    this.partitioner = shuffleDependency.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL,
        RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE);
    this.sendCheckTimeout = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT,
        RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE);
    this.blockNumPerTaskPartition = sparkConf.getInt(RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_TASK_PARTITION,
        RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_TASK_PARTITION_DEFAULT_VALUE);
    this.sendSizeLimit = sparkConf.getSizeAsBytes(RssClientConfig.RSS_CLIENT_SEND_SIZE_LIMIT,
        RssClientConfig.RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE);
    this.blockNumPerBitmap = sparkConf.getLong(RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_BITMAP,
        RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_BITMAP_DEFAULT_VALUE);
    this.partitionToBlockIds = Maps.newConcurrentMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = rssHandle.getShuffleServersForData();
    this.partitionLengths = new long[partitioner.numPartitions()];
    Arrays.fill(partitionLengths, 0);
    partitionToServers = rssHandle.getPartitionToServers();
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    List<ShuffleBlockInfo> shuffleBlockInfos = null;
    Set<Long> blockIds = Sets.newConcurrentHashSet();
    while (records.hasNext()) {
      Product2<K, V> record = records.next();
      K key = record._1();
      int partition = getPartition(key);
      boolean isCombine = shuffleDependency.mapSideCombine();
      if (isCombine) {
        Function1 createCombiner = shuffleDependency.aggregator().get().createCombiner();
        Object c = createCombiner.apply(record._2());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), c);
      } else {
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
        processShuffleBlockInfos(shuffleBlockInfos, blockIds);
      }
    }
    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear();
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
      processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    }
    long checkStartTs = System.currentTimeMillis();
    checkBlockSendResult(blockIds);
    long commitStartTs = System.currentTimeMillis();
    long checkDuration = commitStartTs - checkStartTs;
    sendCommit();
    long writeDurationMs = bufferManager.getWriteTime() + (System.currentTimeMillis() - start);
    shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
    LOG.info("Finish write shuffle for appId[" + appId + "], shuffleId[" + shuffleId
        + "], taskId[" + taskId + "] with write " + writeDurationMs + " ms, include checkSendResult["
        + checkDuration + "], commit[" + (System.currentTimeMillis() - commitStartTs) + "], "
        + bufferManager.getManagerCostInfo());
  }

  private void processShuffleBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfoList, Set<Long> blockIds) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.forEach(sbi -> {
        long blockId = sbi.getBlockId();
        // add blockId to set, check if it is send later
        blockIds.add(blockId);
        // update [partition, blockIds], it will be sent to shuffle server
        int partitionId = sbi.getPartitionId();
        partitionToBlockIds.putIfAbsent(partitionId, Sets.newConcurrentHashSet());
        partitionToBlockIds.get(partitionId).add(blockId);
        partitionLengths[partitionId] += sbi.getLength();
      });
      postBlockEvent(shuffleBlockInfoList);
    }
  }

  protected void postBlockEvent(List<ShuffleBlockInfo> shuffleBlockInfoList) {
    long totalSize = 0;
    List<ShuffleBlockInfo> shuffleBlockInfosPerEvent = Lists.newArrayList();
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      totalSize += sbi.getSize();
      shuffleBlockInfosPerEvent.add(sbi);
      // split shuffle data according to the size
      if (totalSize > sendSizeLimit) {
        LOG.info("Post event to queue with " + shuffleBlockInfosPerEvent.size()
            + " blocks and " + totalSize + " bytes");
        shuffleManager.postEvent(
            new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
        shuffleBlockInfosPerEvent = Lists.newArrayList();
        totalSize = 0;
      }
    }
    if (!shuffleBlockInfosPerEvent.isEmpty()) {
      LOG.info("Post event to queue with " + shuffleBlockInfosPerEvent.size()
          + " blocks and " + totalSize + " bytes");
      shuffleManager.postEvent(
          new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
    }
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) throws RuntimeException {
    long start = System.currentTimeMillis();
    while (true) {
      Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
      Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);

      if (!failedBlockIds.isEmpty()) {
        String errorMsg = "Send failed: Task[" + taskId + "]"
            + " failed because " + failedBlockIds.size()
            + " blocks can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      blockIds.removeAll(successBlockIds);
      if (blockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + blockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MICROSECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg = "Timeout: Task[" + taskId + "] failed because " + blockIds.size()
            + " blocks can't be sent to shuffle server in " + sendCheckTimeout + " ms.";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future = executor.submit(
        () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info("Wait commit to shuffle server for task[" + taskAttemptId + "] cost "
          + (System.currentTimeMillis() - start) + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MICROSECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RuntimeException("Failed to commit task to shuffle server");
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception happened when get commit status", e);
    }
  }

  @VisibleForTesting
  protected <K> int getPartition(K key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (success) {
      try {
        Map<Integer, List<Long>> ptb = Maps.newHashMap();
        int bitmapNum = ClientUtils.getBitmapNum(numMaps, numPartitions, blockNumPerTaskPartition, blockNumPerBitmap);
        for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
          ptb.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
        long start = System.currentTimeMillis();
        shuffleWriteClient.reportShuffleResult(partitionToServers, appId, shuffleId, taskAttemptId, ptb,
            bitmapNum);
        LOG.info("Report application " + appId + "shuffle result for task[" + taskAttemptId
            + "] with bitmapNum[" + bitmapNum + "] with partitionToBlocksSize[" + ptb.size() + "] cost "
            + (System.currentTimeMillis() - start) + " ms");
        // todo: we can replace the dummy host and port with the real shuffle server which we prefer to read
        final BlockManagerId blockManagerId = BlockManagerId.apply(appId + "_" + taskId,
            DUMMY_HOST,
            DUMMY_PORT,
            Option.apply(Long.toString(taskAttemptId)));
        MapStatus mapStatus = MapStatus.apply(blockManagerId, partitionLengths, taskAttemptId);
        return Option.apply(mapStatus);
      } catch (Exception e) {
        LOG.error("Error when stop task.", e);
        throw new RuntimeException(e);
      }
    } else {
      return Option.empty();
    }
  }

  @VisibleForTesting
  Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
