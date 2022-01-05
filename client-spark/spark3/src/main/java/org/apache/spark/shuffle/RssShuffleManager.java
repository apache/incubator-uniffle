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

package org.apache.spark.shuffle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.shuffle.writer.WriteBufferManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.EventLoop;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;

public class RssShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final String clientType;
  private final long heartbeatInterval;
  private final long heartbeatTimeout;
  private final ThreadPoolExecutor threadPoolExecutor;
  private AtomicReference<String> id = new AtomicReference<>();
  private SparkConf sparkConf;
  private int dataReplica;
  private ShuffleWriteClient shuffleWriteClient;
  private final Map<String, Set<Long>> taskToSuccessBlockIds;
  private final Map<String, Set<Long>> taskToFailedBlockIds;
  private Map<String, WriteBufferManager> taskToBufferManager = Maps.newConcurrentMap();
  private final ScheduledExecutorService scheduledExecutorService;
  private boolean heartbeatStarted = false;
  private final EventLoop eventLoop;
  private final EventLoop defaultEventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {

    @Override
    public void onReceive(AddBlockEvent event) {
      threadPoolExecutor.execute(() -> sendShuffleData(event.getTaskId(), event.getShuffleDataInfoList()));
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.info("Shuffle event loop error...", throwable);
    }

    @Override
    public void onStart() {
      LOG.info("Shuffle event loop start...");
    }

    private void sendShuffleData(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
      try {
        SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(id.get(), shuffleDataInfoList);
        putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
        putBlockId(taskToFailedBlockIds, taskId, result.getFailedBlockIds());
      } finally {
        final AtomicLong releaseSize = new AtomicLong(0);
        shuffleDataInfoList.forEach((sbi) -> releaseSize.addAndGet(sbi.getFreeMemory()));
        taskToBufferManager.get(taskId).freeAllocatedMemory(releaseSize.get());
        LOG.debug("Spark 3.0 finish send data and release " + releaseSize + " bytes");
      }
    }

    private synchronized void putBlockId(
        Map<String, Set<Long>> taskToBlockIds,
        String taskAttemptId,
        Set<Long> blockIds) {
      if (blockIds == null || blockIds.isEmpty()) {
        return;
      }
      taskToBlockIds.putIfAbsent(taskAttemptId, Sets.newConcurrentHashSet());
      taskToBlockIds.get(taskAttemptId).addAll(blockIds);
    }
  };

  public RssShuffleManager(SparkConf conf, boolean isDriver) {
    this.sparkConf = conf;
    this.heartbeatInterval = sparkConf.getLong(RssClientConfig.RSS_HEARTBEAT_INTERVAL,
        RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    this.heartbeatTimeout = sparkConf.getLong(RssClientConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    int retryMax = sparkConf.getInt(RssClientConfig.RSS_CLIENT_RETRY_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    this.clientType = sparkConf.get(RssClientConfig.RSS_CLIENT_TYPE,
        RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    dataReplica = sparkConf.getInt(RssClientConfig.RSS_DATA_REPLICA,
        RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    long retryIntervalMax = sparkConf.getLong(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    int heartBeatThreadNum = sparkConf.getInt(RssClientConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
        RssClientConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    shuffleWriteClient = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax, heartBeatThreadNum);
    registerCoordinator();
    taskToSuccessBlockIds = Maps.newConcurrentMap();
    taskToFailedBlockIds = Maps.newConcurrentMap();
    // for non-driver executor, start a thread for sending shuffle data to shuffle server
    LOG.info("RSS data send thread is starting");
    eventLoop = defaultEventLoop;
    eventLoop.start();
    int poolSize = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE,
        RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE);
    int keepAliveTime = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE,
        RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize * 2, keepAliveTime, TimeUnit.SECONDS,
        Queues.newLinkedBlockingQueue(Integer.MAX_VALUE));
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  // For testing only
  @VisibleForTesting
  RssShuffleManager(
      SparkConf conf,
      boolean isDriver,
      EventLoop<AddBlockEvent> loop,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, Set<Long>> taskToFailedBlockIds) {
    this.sparkConf = conf;
    this.clientType = sparkConf.get(RssClientConfig.RSS_CLIENT_TYPE,
        RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    this.heartbeatInterval = sparkConf.getLong(RssClientConfig.RSS_HEARTBEAT_INTERVAL,
        RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    this.heartbeatTimeout = sparkConf.getLong(RssClientConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    int retryMax = sparkConf.getInt(RssClientConfig.RSS_CLIENT_RETRY_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = sparkConf.getLong(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    int heartBeatThreadNum = sparkConf.getInt(RssClientConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
        RssClientConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    dataReplica = sparkConf.getInt(RssClientConfig.RSS_DATA_REPLICA,
        RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    shuffleWriteClient = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax, heartBeatThreadNum);
    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.taskToFailedBlockIds = taskToFailedBlockIds;
    if (loop != null) {
      eventLoop = loop;
    } else {
      eventLoop = defaultEventLoop;
    }
    eventLoop.start();
    threadPoolExecutor = null;
    scheduledExecutorService = null;
  }


  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader)
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, ShuffleDependency<K, V, C> dependency) {

    if (id.get() == null) {
      id.compareAndSet(null, SparkEnv.get().conf().getAppId() + System.currentTimeMillis());
    }
    LOG.info("Generate application id used in rss: " + id.get());
    ShuffleAssignmentsInfo response = shuffleWriteClient.getShuffleAssignments(
        id.get(),
        shuffleId,
        dependency.partitioner().numPartitions(),
        1,
        dataReplica,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = response.getPartitionToServers();

    registerShuffleServers(id.get(), shuffleId, response.getServerToPartitionRanges());
    startHeartbeat();

    LOG.info("RegisterShuffle with ShuffleId[" + shuffleId + "], partitionNum[" + partitionToServers.size()
        + "], shuffleServerForResult: " + partitionToServers);
    return new RssShuffleHandle(shuffleId,
        id.get(),
        dependency.rdd().getNumPartitions(),
        dependency,
        partitionToServers);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle,
      long mapId,
      TaskContext context,
      ShuffleWriteMetricsReporter metrics) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle rssHandle = (RssShuffleHandle) handle;
    // todo: this implement is tricky, we should refactor it
    if (id.get() == null) {
      id.compareAndSet(null, rssHandle.getAppId());
    }
    int shuffleId = rssHandle.getShuffleId();
    String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
    ShuffleWriteMetrics writeMetrics;
    if (metrics != null) {
      writeMetrics = new WriteMetrics(metrics);
    } else {
      writeMetrics = context.taskMetrics().shuffleWriteMetrics();
    }
    WriteBufferManager bufferManager = new WriteBufferManager(
        shuffleId, context.taskAttemptId(), bufferOptions, rssHandle.getDependency().serializer(),
        rssHandle.getPartitionToServers(), context.taskMemoryManager(),
        writeMetrics);
    taskToBufferManager.put(taskId, bufferManager);
    LOG.info("RssHandle appId {} shuffleId {} ", rssHandle.getAppId(), rssHandle.getShuffleId());
    return new RssShuffleWriter(rssHandle.getAppId(), shuffleId, taskId, context.taskAttemptId(), bufferManager,
        writeMetrics, this, sparkConf, shuffleWriteClient, rssHandle);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return getReader(handle, 0, Integer.MAX_VALUE, startPartition, endPartition,
      context, metrics);
  }

  // The interface is used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    long start = System.currentTimeMillis();
    Roaring64NavigableMap taskIdBitmap = getExpectedTasksByExecutorId(
      handle.shuffleId(),
      startPartition,
      endPartition,
      startMapIndex,
      endMapIndex);
    LOG.info("Get taskId cost " + (System.currentTimeMillis() - start) + " ms, and request expected blockIds from "
      + taskIdBitmap.getLongCardinality() + " tasks for shuffleId[" + handle.shuffleId() + "], partitionId["
      + startPartition + "]");
    return getReaderImpl(handle, startMapIndex, endMapIndex, startPartition, endPartition,
        context, metrics, taskIdBitmap);
  }

  // The interface is used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    long start = System.currentTimeMillis();
    Roaring64NavigableMap taskIdBitmap = getExpectedTasksByRange(
      handle.shuffleId(),
      startPartition,
      endPartition,
      startMapIndex,
      endMapIndex);
    LOG.info("Get taskId cost " + (System.currentTimeMillis() - start) + " ms, and request expected blockIds from "
      + taskIdBitmap.getLongCardinality() + " tasks for shuffleId[" + handle.shuffleId() + "], partitionId["
      + startPartition + "]");
    return getReaderImpl(handle, startMapIndex, endMapIndex, startPartition, endPartition,
        context, metrics, taskIdBitmap);
  }

  public <K, C> ShuffleReader<K, C> getReaderImpl(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics,
      Roaring64NavigableMap taskIdBitmap) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    final String shuffleDataBasePath = sparkConf.get(RssClientConfig.RSS_BASE_PATH, "");
    final String storageType = sparkConf.get(RssClientConfig.RSS_STORAGE_TYPE);
    final int indexReadLimit = sparkConf.getInt(RssClientConfig.RSS_INDEX_READ_LIMIT,
        RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
    RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
    final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
    long readBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE,
        RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
    if (readBufferSize > Integer.MAX_VALUE) {
      LOG.warn(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE + " can support 2g as max");
      readBufferSize = Integer.MAX_VALUE;
    }
    int shuffleId = rssShuffleHandle.getShuffleId();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =  rssShuffleHandle.getPartitionToServers();
    Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks = new HashMap<>();
    for (int partition = startPartition; partition < endPartition; partition++) {
      long start = System.currentTimeMillis();
      Roaring64NavigableMap blockIdBitmap = shuffleWriteClient.getShuffleResult(
          clientType, Sets.newHashSet(partitionToServers.get(partition)),
          rssShuffleHandle.getAppId(), shuffleId, partition);
      partitionToExpectBlocks.put(partition, blockIdBitmap);
      LOG.info("Get shuffle blockId cost " + (System.currentTimeMillis() - start) + " ms, and get "
          + blockIdBitmap.getLongCardinality() + " blockIds for shuffleId[" + shuffleId + "], partitionId["
          + startPartition + "]");
    }

    ShuffleReadMetrics readMetrics;
    if (metrics != null) {
      readMetrics = new ReadMetrics(metrics);
    } else {
      readMetrics = context.taskMetrics().shuffleReadMetrics();
    }
    return new RssShuffleReader<K, C>(
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        rssShuffleHandle,
        shuffleDataBasePath,
        indexReadLimit,
        RssShuffleUtils.newHadoopConfiguration(sparkConf),
        storageType,
        (int) readBufferSize,
        partitionNum,
        partitionToExpectBlocks,
        taskIdBitmap,
        readMetrics);
  }

  private Roaring64NavigableMap getExpectedTasksByExecutorId(
      int shuffleId,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    // Since Spark 3.1 refactors the interface of getMapSizesByExecutorId,
    // we use reflection and catch for the compatibility with 3.0 & 3.1
    try {
      // attempt to use Spark 3.1's API
      mapStatusIter = (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
          SparkEnv.get().mapOutputTracker().getClass()
              .getDeclaredMethod("getMapSizesByExecutorId",
                  int.class, int.class, int.class, int.class, int.class)
              .invoke(SparkEnv.get().mapOutputTracker(),
                  shuffleId,
                  startMapIndex,
                  endMapIndex,
                  startPartition,
                  endPartition);
    } catch (Exception e) {
      // fallback and attempt to use Spark 3.0's API
      try {
        mapStatusIter = (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
            SparkEnv.get().mapOutputTracker().getClass()
                .getDeclaredMethod("getMapSizesByExecutorId",
                    int.class, int.class, int.class)
                .invoke(
                    SparkEnv.get().mapOutputTracker(),
                    shuffleId,
                    startPartition,
                    endPartition);
      } catch (Exception ee) {
        throw new RuntimeException(ee);
      }
    }
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> tuple2 = mapStatusIter.next();
      if (!tuple2._1().topologyInfo().isDefined()) {
        throw new RuntimeException("Can't get expected taskAttemptId");
      }
      taskIdBitmap.add(Long.parseLong(tuple2._1().topologyInfo().get()));
    }
    return taskIdBitmap;
  }

  // This API is only used by Spark3.0 and removed since 3.1,
  // so we extract it from getExpectedTasksByExecutorId.
  private Roaring64NavigableMap getExpectedTasksByRange(
    int shuffleId,
    int startPartition,
    int endPartition,
    int startMapIndex,
    int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    try {
      mapStatusIter = (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
        SparkEnv.get().mapOutputTracker().getClass()
          .getDeclaredMethod("getMapSizesByRange",
            int.class, int.class, int.class, int.class, int.class)
          .invoke(SparkEnv.get().mapOutputTracker(),
            shuffleId,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> tuple2 = mapStatusIter.next();
      if (!tuple2._1().topologyInfo().isDefined()) {
        throw new RuntimeException("Can't get expected taskAttemptId");
      }
      taskIdBitmap.add(Long.parseLong(tuple2._1().topologyInfo().get()));
    }
    return taskIdBitmap;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return true;
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RuntimeException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  @Override
  public void stop() {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    if (threadPoolExecutor != null) {
      threadPoolExecutor.shutdownNow();
    }
    if (shuffleWriteClient != null) {
      shuffleWriteClient.close();
    }
    if (eventLoop != null) {
      eventLoop.stop();
    }
  }

  public void clearTaskMeta(String taskId) {
    taskToSuccessBlockIds.remove(taskId);
    taskToFailedBlockIds.remove(taskId);
    taskToBufferManager.remove(taskId);
  }

  @VisibleForTesting
  protected void registerShuffleServers(String appId, int shuffleId,
                                        Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId[" + shuffleId + "]");
    long start = System.currentTimeMillis();
    Set<Map.Entry<ShuffleServerInfo, List<PartitionRange>>> entries = serverToPartitionRanges.entrySet();
    entries.parallelStream()
        .forEach(entry -> {
          shuffleWriteClient.registerShuffle(
              entry.getKey(),
              appId,
              shuffleId,
              entry.getValue());
        });
    LOG.info("Finish register shuffleId[" + shuffleId + "] with " + (System.currentTimeMillis() - start) + " ms");
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String coordinators = sparkConf.get(RssClientConfig.RSS_COORDINATOR_QUORUM);
    LOG.info("Start Registering coordinators {}", coordinators);
    shuffleWriteClient.registerCoordinators(coordinators);
  }

  private synchronized void startHeartbeat() {
    if (!heartbeatStarted) {
      scheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              shuffleWriteClient.sendAppHeartbeat(id.get(), heartbeatTimeout);
              LOG.info("Finish send heartbeat to coordinator and servers");
            } catch (Exception e) {
              LOG.warn("Fail to send heartbeat to coordinator and servers", e);
            }
          },
          heartbeatInterval / 2,
          heartbeatInterval,
          TimeUnit.MILLISECONDS);
      heartbeatStarted = true;
    }
  }

  public void postEvent(AddBlockEvent addBlockEvent) {
    if (eventLoop != null) {
      eventLoop.post(addBlockEvent);
    }
  }

  public Set<Long> getFailedBlockIds(String taskId) {
    Set<Long> result = taskToFailedBlockIds.get(taskId);
    if (result == null) {
      result = Sets.newHashSet();
    }
    return result;
  }

  public Set<Long> getSuccessBlockIds(String taskId) {
    Set<Long> result = taskToSuccessBlockIds.get(taskId);
    if (result == null) {
      result = Sets.newHashSet();
    }
    return result;
  }

  class ReadMetrics extends ShuffleReadMetrics {
    private ShuffleReadMetricsReporter reporter;

    ReadMetrics(ShuffleReadMetricsReporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void incRemoteBytesRead(long v) {
      reporter.incRemoteBytesRead(v);
    }

    @Override
    public void incFetchWaitTime(long v) {
      reporter.incFetchWaitTime(v);
    }

    @Override
    public void incRecordsRead(long v) {
      reporter.incRecordsRead(v);
    }
  }

  class WriteMetrics extends ShuffleWriteMetrics {
    private ShuffleWriteMetricsReporter reporter;

    WriteMetrics(ShuffleWriteMetricsReporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void incBytesWritten(long v) {
      reporter.incBytesWritten(v);
    }

    @Override
    public void incRecordsWritten(long v) {
      reporter.incRecordsWritten(v);
    }

    @Override
    public void incWriteTime(long v) {
      reporter.incWriteTime(v);
    }
  }
}
