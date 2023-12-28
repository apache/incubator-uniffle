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

package org.apache.spark.shuffle.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.ShuffleHandleInfo;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.request.RssReassignServersRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.client.response.RssReassignServersReponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteFailureResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssSendFailedException;
import org.apache.uniffle.common.exception.RssWaitFailedException;
import org.apache.uniffle.storage.util.StorageType;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);
  private static final String DUMMY_HOST = "dummy_host";
  private static final int DUMMY_PORT = 99999;

  private final String appId;
  private final int shuffleId;
  private WriteBufferManager bufferManager;
  private final String taskId;
  private final int numMaps;
  private final ShuffleDependency<K, V, C> shuffleDependency;
  private final Partitioner partitioner;
  private final RssShuffleManager shuffleManager;
  private final boolean shouldPartition;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final int bitmapSplitNum;
  private final Map<Integer, Set<Long>> partitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final Set<ShuffleServerInfo> shuffleServersForData;
  private final long[] partitionLengths;
  private final boolean isMemoryShuffleEnabled;
  private final Function<String, Boolean> taskFailureCallback;
  private final Set<Long> blockIds = Sets.newConcurrentHashSet();
  private TaskContext taskContext;
  private SparkConf sparkConf;

  /** used by columnar rss shuffle writer implementation */
  protected final long taskAttemptId;

  protected final ShuffleWriteMetrics shuffleWriteMetrics;

  private final BlockingQueue<Object> finishEventQueue = new LinkedBlockingQueue<>();

  // Only for tests
  @VisibleForTesting
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
      RssShuffleHandle<K, V, C> rssHandle,
      ShuffleHandleInfo shuffleHandleInfo,
      TaskContext context) {
    this(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        (tid) -> true,
        shuffleHandleInfo,
        context);
    this.bufferManager = bufferManager;
  }

  private RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle<K, V, C> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      ShuffleHandleInfo shuffleHandleInfo,
      TaskContext context) {
    LOG.warn("RssShuffle start write taskAttemptId data" + taskAttemptId);
    this.shuffleManager = shuffleManager;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.shuffleDependency = rssHandle.getDependency();
    this.partitioner = shuffleDependency.partitioner();
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckTimeout = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    this.sendCheckInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    this.bitmapSplitNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM);
    this.partitionToBlockIds = Maps.newHashMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = shuffleHandleInfo.getShuffleServersForData();
    this.partitionLengths = new long[partitioner.numPartitions()];
    Arrays.fill(partitionLengths, 0);
    partitionToServers = shuffleHandleInfo.getPartitionToServers();
    this.isMemoryShuffleEnabled =
        isMemoryShuffleEnabled(sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key()));
    this.taskFailureCallback = taskFailureCallback;
    this.taskContext = context;
    this.sparkConf = sparkConf;
  }

  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle<K, V, C> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      TaskContext context,
      ShuffleHandleInfo shuffleHandleInfo) {
    this(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        taskFailureCallback,
        shuffleHandleInfo,
        context);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
    final WriteBufferManager bufferManager =
        new WriteBufferManager(
            shuffleId,
            taskId,
            taskAttemptId,
            bufferOptions,
            rssHandle.getDependency().serializer(),
            shuffleHandleInfo.getPartitionToServers(),
            context.taskMemoryManager(),
            shuffleWriteMetrics,
            RssSparkConfig.toRssConf(sparkConf),
            this::processShuffleBlockInfos);
    this.bufferManager = bufferManager;
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.withMemory(StorageType.valueOf(storageType));
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) {
    try {
      writeImpl(records);
    } catch (Exception e) {
      taskFailureCallback.apply(taskId);
      if (shuffleManager.isRssResubmitStage()) {
        throwFetchFailedIfNecessary(e);
      } else {
        throw e;
      }
    }
  }

  private void writeImpl(Iterator<Product2<K, V>> records) {
    List<ShuffleBlockInfo> shuffleBlockInfos;
    boolean isCombine = shuffleDependency.mapSideCombine();
    Function1<V, C> createCombiner = null;
    if (isCombine) {
      createCombiner = shuffleDependency.aggregator().get().createCombiner();
    }
    while (records.hasNext()) {
      // Task should fast fail when sending data failed
      checkIfBlocksFailed();

      Product2<K, V> record = records.next();
      K key = record._1();
      int partition = getPartition(key);
      if (isCombine) {
        Object c = createCombiner.apply(record._2());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), c);
      } else {
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
        processShuffleBlockInfos(shuffleBlockInfos);
      }
    }
    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear();
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
      processShuffleBlockInfos(shuffleBlockInfos);
    }
    long checkStartTs = System.currentTimeMillis();
    checkBlockSendResult(blockIds);
    long commitStartTs = System.currentTimeMillis();
    long checkDuration = commitStartTs - checkStartTs;
    if (!isMemoryShuffleEnabled) {
      sendCommit();
    }
    long writeDurationMs = bufferManager.getWriteTime() + (System.currentTimeMillis() - start);
    shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
    LOG.info(
        "Finish write shuffle for appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], taskId["
            + taskId
            + "] with write "
            + writeDurationMs
            + " ms, include checkSendResult["
            + checkDuration
            + "], commit["
            + (System.currentTimeMillis() - commitStartTs)
            + "], "
            + bufferManager.getManagerCostInfo());
  }

  // only push-based shuffle use this interface, but rss won't be used when push-based shuffle is
  // enabled.
  public long[] getPartitionLengths() {
    return new long[0];
  }

  @VisibleForTesting
  protected List<CompletableFuture<Long>> processShuffleBlockInfos(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.forEach(
          sbi -> {
            long blockId = sbi.getBlockId();
            // add blockId to set, check if it is sent later
            blockIds.add(blockId);
            // update [partition, blockIds], it will be sent to shuffle server
            int partitionId = sbi.getPartitionId();
            partitionToBlockIds.computeIfAbsent(partitionId, k -> Sets.newHashSet()).add(blockId);
            partitionLengths[partitionId] += sbi.getLength();
          });
      return postBlockEvent(shuffleBlockInfoList);
    }
    return Collections.emptyList();
  }

  protected List<CompletableFuture<Long>> postBlockEvent(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (AddBlockEvent event : bufferManager.buildBlockEvents(shuffleBlockInfoList)) {
      event.addCallback(
          () -> {
            boolean ret = finishEventQueue.add(new Object());
            if (!ret) {
              LOG.error("Add event " + event + " to finishEventQueue fail");
            }
          });
      futures.add(shuffleManager.sendData(event));
    }
    return futures;
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) {
    boolean interrupted = false;

    try {
      long remainingMs = sendCheckTimeout;
      long end = System.currentTimeMillis() + remainingMs;

      while (true) {
        try {
          finishEventQueue.clear();
          checkIfBlocksFailed();
          Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
          blockIds.removeAll(successBlockIds);
          if (blockIds.isEmpty()) {
            break;
          }
          if (finishEventQueue.isEmpty()) {
            remainingMs = Math.max(end - System.currentTimeMillis(), 0);
            Object event = finishEventQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
            if (event == null) {
              break;
            }
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (!blockIds.isEmpty()) {
        String errorMsg =
            "Timeout: Task["
                + taskId
                + "] failed because "
                + blockIds.size()
                + " blocks can't be sent to shuffle server in "
                + sendCheckTimeout
                + " ms.";
        LOG.error(errorMsg);
        throw new RssWaitFailedException(errorMsg);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void checkIfBlocksFailed() {
    Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);
    if (!failedBlockIds.isEmpty()) {
      String errorMsg =
          "Send failed: Task["
              + taskId
              + "]"
              + " failed because "
              + failedBlockIds.size()
              + " blocks can't be sent to shuffle server.";
      LOG.error(errorMsg);
      throw new RssSendFailedException(errorMsg);
    }
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        executor.submit(
            () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task["
              + taskAttemptId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  @VisibleForTesting
  protected <T> int getPartition(T key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (success) {
        Map<Integer, List<Long>> ptb = Maps.newHashMap();
        for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
          ptb.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
        long start = System.currentTimeMillis();
        shuffleWriteClient.reportShuffleResult(
            partitionToServers, appId, shuffleId, taskAttemptId, ptb, bitmapSplitNum);
        LOG.info(
            "Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
            taskAttemptId,
            bitmapSplitNum,
            (System.currentTimeMillis() - start));
        // todo: we can replace the dummy host and port with the real shuffle server which we prefer
        // to read
        final BlockManagerId blockManagerId =
            BlockManagerId.apply(
                appId + "_" + taskId,
                DUMMY_HOST,
                DUMMY_PORT,
                Option.apply(Long.toString(taskAttemptId)));
        MapStatus mapStatus = MapStatus.apply(blockManagerId, partitionLengths, taskAttemptId);
        return Option.apply(mapStatus);
      } else {
        return Option.empty();
      }
    } finally {
      // free all memory & metadata, or memory leak happen in executor
      if (bufferManager != null) {
        bufferManager.freeAllMemory();
      }
      if (shuffleManager != null) {
        shuffleManager.clearTaskMeta(taskId);
      }
    }
  }

  @VisibleForTesting
  Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }

  @VisibleForTesting
  public WriteBufferManager getBufferManager() {
    return bufferManager;
  }

  private static ShuffleManagerClient createShuffleManagerClient(String host, int port)
      throws IOException {
    ClientType grpc = ClientType.GRPC;
    // Host can be inferred from `spark.driver.bindAddress`, which would be set when SparkContext is
    // constructed.
    return ShuffleManagerClientFactory.getInstance().createShuffleManagerClient(grpc, host, port);
  }

  private void throwFetchFailedIfNecessary(Exception e) {
    // The shuffleServer is registered only when a Block fails to be sent
    if (e instanceof RssSendFailedException) {
      Map<Long, BlockingQueue<ShuffleServerInfo>> failedBlockIds =
          shuffleManager.getFailedBlockIdsWithShuffleServer(taskId);
      List<ShuffleServerInfo> shuffleServerInfos = Lists.newArrayList();
      for (Map.Entry<Long, BlockingQueue<ShuffleServerInfo>> longListEntry :
          failedBlockIds.entrySet()) {
        shuffleServerInfos.addAll(longListEntry.getValue());
      }
      RssReportShuffleWriteFailureRequest req =
          new RssReportShuffleWriteFailureRequest(
              appId,
              shuffleId,
              taskContext.stageAttemptNumber(),
              shuffleServerInfos,
              e.getMessage());
      RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
      String driver = rssConf.getString("driver.host", "");
      int port = rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT);
      try (ShuffleManagerClient shuffleManagerClient = createShuffleManagerClient(driver, port)) {
        RssReportShuffleWriteFailureResponse response =
            shuffleManagerClient.reportShuffleWriteFailure(req);
        if (response.getReSubmitWholeStage()) {
          RssReassignServersRequest rssReassignServersRequest =
              new RssReassignServersRequest(
                  taskContext.stageId(),
                  taskContext.stageAttemptNumber(),
                  shuffleId,
                  partitioner.numPartitions());
          RssReassignServersReponse rssReassignServersReponse =
              shuffleManagerClient.reassignShuffleServers(rssReassignServersRequest);
          LOG.info(
              "Whether the reassignment is successful: {}",
              rssReassignServersReponse.isNeedReassign());
          // since we are going to roll out the whole stage, mapIndex shouldn't matter, hence -1 is
          // provided.
          FetchFailedException ffe =
              RssSparkShuffleUtils.createFetchFailedException(
                  shuffleId, -1, taskContext.stageAttemptNumber(), e);
          throw new RssException(ffe);
        }
      } catch (IOException ioe) {
        LOG.info("Error closing shuffle manager client with error:", ioe);
      }
    }
    throw new RssException(e);
  }
}
