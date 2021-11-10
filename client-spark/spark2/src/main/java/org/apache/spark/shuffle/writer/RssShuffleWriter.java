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
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import com.tencent.rss.common.exception.RssException;
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
  // they will be used in commit phase
  private final Set<ShuffleServerInfo> shuffleServersForData;
  private final Map<Integer, Set<Long>> partitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private String appId;
  private int numMaps;
  private int numPartitions;
  private int shuffleId;
  private int blockNumPerTaskPartition;
  private long blockNumPerBitmap;
  private String taskId;
  private long taskAttemptId;
  private ShuffleDependency<K, V, C> shuffleDependency;
  private ShuffleWriteMetrics shuffleWriteMetrics;
  private Partitioner partitioner;
  private boolean shouldPartition;
  private WriteBufferManager bufferManager;
  private RssShuffleManager shuffleManager;
  private long sendCheckTimeout;
  private long sendCheckInterval;
  private long sendSizeLimit;

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
    this.appId = appId;
    this.bufferManager = bufferManager;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleDependency = rssHandle.getDependency();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.partitioner = shuffleDependency.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.shuffleManager = shuffleManager;
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckTimeout = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT,
        RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE);
    this.sendCheckInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL,
        RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE);
    this.sendSizeLimit = sparkConf.getSizeAsBytes(RssClientConfig.RSS_CLIENT_SEND_SIZE_LIMIT,
        RssClientConfig.RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE);
    this.blockNumPerTaskPartition = sparkConf.getInt(RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_TASK_PARTITION,
        RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_TASK_PARTITION_DEFAULT_VALUE);
    this.blockNumPerBitmap = sparkConf.getLong(RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_BITMAP,
        RssClientConfig.RSS_CLIENT_BLOCK_NUM_PER_BITMAP_DEFAULT_VALUE);
    this.partitionToBlockIds = Maps.newConcurrentMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = rssHandle.getShuffleServersForData();
    this.partitionToServers = rssHandle.getPartitionToServers();
  }

  /**
   * Create dummy BlockManagerId and embed partition->blockIds
   */
  private BlockManagerId createDummyBlockManagerId(String executorId, long taskAttemptId) {
    // dummy values are used there for host and port check in BlockManagerId
    // hack: use topologyInfo field in BlockManagerId to store [partition, blockIds]
    return BlockManagerId.apply(executorId, DUMMY_HOST, DUMMY_PORT, Option.apply(Long.toString(taskAttemptId)));
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) {
    List<ShuffleBlockInfo> shuffleBlockInfos = null;
    Set<Long> blockIds = Sets.newConcurrentHashSet();
    while (records.hasNext()) {
      Product2<K, V> record = records.next();
      int partition = getPartition(record._1());
      if (shuffleDependency.mapSideCombine()) {
        Function1 createCombiner = shuffleDependency.aggregator().get().createCombiner();
        Object c = createCombiner.apply(record._2());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), c);
      } else {
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    }

    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear();
    processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    long s = System.currentTimeMillis();
    checkBlockSendResult(blockIds);
    final long checkDuration = System.currentTimeMillis() - s;
    s = System.currentTimeMillis();
    sendCommit();
    final long commitDuration = System.currentTimeMillis() - s;
    long writeDurationMs = bufferManager.getWriteTime() + (System.currentTimeMillis() - start);
    shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
    LOG.info("Finish write shuffle for appId[" + appId + "], shuffleId[" + shuffleId
        + "], taskId[" + taskId + "] with write " + writeDurationMs + " ms, include checkSendResult["
        + checkDuration + "], commit[" + commitDuration + "], " + bufferManager.getManagerCostInfo());
  }

  /**
   * ShuffleBlock will be added to queue and send to shuffle server
   * maintenance the following information:
   * 1. add blockId to set, check if it is send later
   * 2. update shuffle server info, they will be used in commit phase
   * 3. update [partition, blockIds], it will be set to MapStatus,
   * and shuffle reader will do the integration check with them
   *
   * @param shuffleBlockInfoList
   * @param blockIds
   */
  private void processShuffleBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfoList, Set<Long> blockIds) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.parallelStream().forEach(sbi -> {
        long blockId = sbi.getBlockId();
        // add blockId to set, check if it is send later
        blockIds.add(blockId);
        // update [partition, blockIds], it will be sent to shuffle server
        int partitionId = sbi.getPartitionId();
        partitionToBlockIds.putIfAbsent(partitionId, Sets.newConcurrentHashSet());
        partitionToBlockIds.get(partitionId).add(blockId);
      });
      postBlockEvent(shuffleBlockInfoList);
    }
  }

  // don't send huge block to shuffle server, or there will be OOM if shuffle sever receives data more than expected
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
        shuffleManager.getEventLoop().post(
            new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
        shuffleBlockInfosPerEvent = Lists.newArrayList();
        totalSize = 0;
      }
    }
    if (!shuffleBlockInfosPerEvent.isEmpty()) {
      LOG.info("Post event to queue with " + shuffleBlockInfosPerEvent.size()
          + " blocks and " + totalSize + " bytes");
      shuffleManager.getEventLoop().post(
          new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
    }
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future = executor.submit(
        () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
    while (!future.isDone()) {
      LOG.info("Wait commit to shuffle server for task[" + taskAttemptId + "] cost "
          + (System.currentTimeMillis() - start) + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MICROSECONDS);
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
    }
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) throws RuntimeException {
    long start = System.currentTimeMillis();
    while (true) {
      Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);
      Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
      // if failed when send data to shuffle server, mark task as failed
      if (failedBlockIds.size() > 0) {
        String errorMsg =
            "Send failed: Task[" + taskId + "] failed because " + failedBlockIds.size()
                + " blocks can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }

      // remove blockIds which was sent successfully, if there has none left, all data are sent
      blockIds.removeAll(successBlockIds);
      if (blockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + blockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MICROSECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
            "Timeout: Task[" + taskId + "] failed because " + blockIds.size()
                + " blocks can't be sent to shuffle server in " + sendCheckTimeout + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (success) {
      try {
        // fill partitionLengths with non zero dummy value so map output tracker could work correctly
        long[] partitionLengths = new long[partitioner.numPartitions()];
        Arrays.fill(partitionLengths, 1);
        final BlockManagerId blockManagerId =
            createDummyBlockManagerId(appId + "_" + taskId, taskAttemptId);

        Map<Integer, List<Long>> ptb = Maps.newHashMap();
        for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
          ptb.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
        long start = System.currentTimeMillis();
        int bitmapNum = ClientUtils.getBitmapNum(numMaps, numPartitions, blockNumPerTaskPartition, blockNumPerBitmap);
        shuffleWriteClient.reportShuffleResult(partitionToServers, appId, shuffleId, taskAttemptId, ptb,
            bitmapNum);
        LOG.info("Report shuffle result for task[" + taskAttemptId + "] with bitmapNum[" + bitmapNum + "] cost "
            + (System.currentTimeMillis() - start) + " ms");
        MapStatus mapStatus = MapStatus$.MODULE$.apply(blockManagerId, partitionLengths);
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
  protected <K> int getPartition(K key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @VisibleForTesting
  protected Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }

  @VisibleForTesting
  protected ShuffleWriteMetrics getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
  }
}
