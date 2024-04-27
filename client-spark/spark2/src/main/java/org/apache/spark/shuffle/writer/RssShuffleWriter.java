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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.uniffle.common.util.BlockId;
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
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
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
  // they will be used in commit phase
  private final Set<ShuffleServerInfo> shuffleServersForData;
  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<BlockId>>> serverToPartitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private String appId;
  private int numMaps;
  private int shuffleId;
  private int bitmapSplitNum;
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
  private boolean isMemoryShuffleEnabled;
  private final Function<String, Boolean> taskFailureCallback;
  private final Set<BlockId> blockIds = Sets.newConcurrentHashSet();
  private TaskContext taskContext;
  private SparkConf sparkConf;

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
      SimpleShuffleHandleInfo shuffleHandleInfo,
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
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleDependency = rssHandle.getDependency();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.partitioner = shuffleDependency.partitioner();
    this.shuffleManager = shuffleManager;
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckTimeout = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    this.sendCheckInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    this.bitmapSplitNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM);
    this.serverToPartitionToBlockIds = Maps.newHashMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = shuffleHandleInfo.getServers();
    this.partitionToServers = shuffleHandleInfo.getAvailablePartitionServersForWriter();
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
            shuffleHandleInfo.getAvailablePartitionServersForWriter(),
            context.taskMemoryManager(),
            shuffleWriteMetrics,
            RssSparkConfig.toRssConf(sparkConf),
            this::processShuffleBlockInfos);
    this.bufferManager = bufferManager;
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.withMemory(StorageType.valueOf(storageType));
  }

  /** Create dummy BlockManagerId and embed partition->blockIds */
  private BlockManagerId createDummyBlockManagerId(String executorId, long taskAttemptId) {
    // dummy values are used there for host and port check in BlockManagerId
    // hack: use topologyInfo field in BlockManagerId to store [partition, blockIds]
    return BlockManagerId.apply(
        executorId, DUMMY_HOST, DUMMY_PORT, Option.apply(Long.toString(taskAttemptId)));
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
    long recordCount = 0;
    while (records.hasNext()) {
      recordCount++;
      Product2<K, V> record = records.next();
      int partition = getPartition(record._1());
      if (shuffleDependency.mapSideCombine()) {
        Function1<V, C> createCombiner = shuffleDependency.aggregator().get().createCombiner();
        Object c = createCombiner.apply(record._2());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), c);
      } else {
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      processShuffleBlockInfos(shuffleBlockInfos);
    }

    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear(1.0);
    processShuffleBlockInfos(shuffleBlockInfos);
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    long s = System.currentTimeMillis();
    checkSentRecordCount(recordCount);
    checkSentBlockCount();
    checkBlockSendResult(blockIds);
    final long checkDuration = System.currentTimeMillis() - s;
    long commitDuration = 0;
    if (!isMemoryShuffleEnabled) {
      s = System.currentTimeMillis();
      sendCommit();
      commitDuration = System.currentTimeMillis() - s;
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
            + commitDuration
            + "], "
            + bufferManager.getManagerCostInfo());
  }

  private void checkSentRecordCount(long recordCount) {
    if (recordCount != bufferManager.getRecordCount()) {
      String errorMsg =
          "Potential record loss may have occurred while preparing to send blocks for task["
              + taskId
              + "]";
      throw new RssSendFailedException(errorMsg);
    }
  }

  private void checkSentBlockCount() {
    long tracked = 0;
    if (serverToPartitionToBlockIds != null) {
      Set<Long> blockIds = new HashSet<>();
      for (Map<Integer, Set<Long>> partitionBlockIds : serverToPartitionToBlockIds.values()) {
        partitionBlockIds.values().forEach(x -> blockIds.addAll(x));
      }
      tracked = blockIds.size();
    }
    if (tracked != bufferManager.getBlockCount()) {
      throw new RssSendFailedException(
          "Potential block loss may occur when preparing to send blocks for task[" + taskId + "]");
    }
  }

  /**
   * ShuffleBlock will be added to queue and send to shuffle server maintenance the following
   * information: 1. add blockId to set, check if it is send later 2. update shuffle server info,
   * they will be used in commit phase 3. update [partition, blockIds], it will be set to MapStatus,
   * and shuffle reader will do the integration check with them
   *
   * @param shuffleBlockInfoList
   */
  private List<CompletableFuture<Long>> processShuffleBlockInfos(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.stream()
          .forEach(
              sbi -> {
                BlockId blockId = sbi.getBlockId();
                // add blockId to set, check if it is send later
                blockIds.add(blockId);
                // update [partition, blockIds], it will be sent to shuffle server
                int partitionId = sbi.getPartitionId();
                sbi.getShuffleServerInfos()
                    .forEach(
                        shuffleServerInfo -> {
                          Map<Integer, Set<BlockId>> pToBlockIds =
                              serverToPartitionToBlockIds.computeIfAbsent(
                                  shuffleServerInfo, k -> Maps.newHashMap());
                          pToBlockIds
                              .computeIfAbsent(partitionId, v -> Sets.newHashSet())
                              .add(blockId);
                        });
              });
      return postBlockEvent(shuffleBlockInfoList);
    }
    return Collections.emptyList();
  }

  // don't send huge block to shuffle server, or there will be OOM if shuffle sever receives data
  // more than expected
  protected List<CompletableFuture<Long>> postBlockEvent(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (AddBlockEvent event : bufferManager.buildBlockEvents(shuffleBlockInfoList)) {
      futures.add(shuffleManager.sendData(event));
    }
    return futures;
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        executor.submit(
            () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
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
      // check if commit/finish rpc is successful
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
  protected void checkBlockSendResult(Set<BlockId> blockIds) {
    long start = System.currentTimeMillis();
    while (true) {
      Set<BlockId> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);
      Set<BlockId> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
      // if failed when send data to shuffle server, mark task as failed
      if (failedBlockIds.size() > 0) {
        String errorMsg =
            "Send failed: Task["
                + taskId
                + "] failed because "
                + failedBlockIds.size()
                + " blocks can't be sent to shuffle server: "
                + shuffleManager.getBlockIdsFailedSendTracker(taskId).getFaultyShuffleServers();
        LOG.error(errorMsg);
        throw new RssSendFailedException(errorMsg);
      }

      // remove blockIds which was sent successfully, if there has none left, all data are sent
      blockIds.removeAll(successBlockIds);
      if (blockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + blockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
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
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (success) {
        // fill partitionLengths with non zero dummy value so map output tracker could work
        // correctly
        long[] partitionLengths = new long[partitioner.numPartitions()];
        Arrays.fill(partitionLengths, 1);
        final BlockManagerId blockManagerId =
            createDummyBlockManagerId(appId + "_" + taskId, taskAttemptId);
        long start = System.currentTimeMillis();
        shuffleWriteClient.reportShuffleResult(
            serverToPartitionToBlockIds, appId, shuffleId, taskAttemptId, bitmapSplitNum);
        LOG.info(
            "Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
            taskAttemptId,
            bitmapSplitNum,
            (System.currentTimeMillis() - start));
        MapStatus mapStatus = MapStatus$.MODULE$.apply(blockManagerId, partitionLengths);
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
  protected <T> int getPartition(T key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @VisibleForTesting
  protected Map<Integer, Set<BlockId>> getPartitionToBlockIds() {
    return serverToPartitionToBlockIds.values().stream()
        .flatMap(s -> s.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existingSet, newSet) -> {
                  Set<BlockId> mergedSet = new HashSet<>(existingSet);
                  mergedSet.addAll(newSet);
                  return mergedSet;
                }));
  }

  @VisibleForTesting
  protected ShuffleWriteMetrics getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
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
      FailedBlockSendTracker blockIdsFailedSendTracker =
          shuffleManager.getBlockIdsFailedSendTracker(taskId);
      List<ShuffleServerInfo> shuffleServerInfos =
          Lists.newArrayList(blockIdsFailedSendTracker.getFaultyShuffleServers());
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
