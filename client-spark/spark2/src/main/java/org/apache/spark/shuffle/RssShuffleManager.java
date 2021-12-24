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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
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
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

public class RssShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final long heartbeatInterval;
  private final long heartbeatTimeout;
  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private SparkConf sparkConf;
  private String appId = "";
  private boolean isDriver;
  private String clientType;
  private ShuffleWriteClient shuffleWriteClient;
  private Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
  private Map<String, Set<Long>> taskToFailedBlockIds = Maps.newConcurrentMap();
  private Map<String, WriteBufferManager> taskToBuffManager = Maps.newConcurrentMap();
  private boolean heartbeatStarted = false;
  private ThreadPoolExecutor threadPoolExecutor;
  private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {

    @Override
    public void onReceive(AddBlockEvent event) {
      threadPoolExecutor.execute(() -> sendShuffleData(event.getTaskId(), event.getShuffleDataInfoList()));
    }

    private void sendShuffleData(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
      try {
        SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(appId, shuffleDataInfoList);
        putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
        putBlockId(taskToFailedBlockIds, taskId, result.getFailedBlockIds());
      } finally {
        // data is already send, release the memory to executor
        long releaseSize = 0;
        for (ShuffleBlockInfo sbi : shuffleDataInfoList) {
          releaseSize += sbi.getFreeMemory();
        }
        taskToBuffManager.get(taskId).freeAllocatedMemory(releaseSize);
        LOG.debug("Finish send data and release " + releaseSize + " bytes");
      }
    }

    private synchronized void putBlockId(
        Map<String, Set<Long>> taskToBlockIds,
        String taskAttemptId,
        Set<Long> blockIds) {
      if (blockIds == null) {
        return;
      }
      if (taskToBlockIds.get(taskAttemptId) == null) {
        taskToBlockIds.put(taskAttemptId, Sets.newConcurrentHashSet());
      }
      taskToBlockIds.get(taskAttemptId).addAll(blockIds);
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onStart() {
    }
  };

  public RssShuffleManager(SparkConf sparkConf, boolean isDriver) {
    this.sparkConf = sparkConf;
    this.isDriver = isDriver;
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
    shuffleWriteClient = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax, heartBeatThreadNum);
    registerCoordinator();
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false)) {
      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data send thread is starting");
      eventLoop.start();
      int poolSize = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE,
          RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE);
      int keepAliveTime = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE,
          RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE);
      threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize * 2, keepAliveTime, TimeUnit.SECONDS,
          Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SendData").build());
    }
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader).
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
    // If yarn enable retry ApplicationMaster, appId will be not unique and shuffle data will be incorrect,
    // appId + timestamp can avoid such problem,
    // can't get appId in construct because SparkEnv is not created yet,
    // appId will be initialized only once in this method which
    // will be called many times depend on how many shuffle stage
    if ("".equals(appId)) {
      appId = SparkEnv.get().conf().getAppId() + "_" + System.currentTimeMillis();
      LOG.info("Generate application id used in rss: " + appId);
    }

    int partitionNumPerRange = sparkConf.getInt(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
        RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
    int dataReplica = sparkConf.getInt(RssClientConfig.RSS_DATA_REPLICA,
        RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    // get all register info according to coordinator's response
    ShuffleAssignmentsInfo response = shuffleWriteClient.getShuffleAssignments(
        appId, shuffleId, dependency.partitioner().numPartitions(),
        partitionNumPerRange, dataReplica, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = response.getPartitionToServers();

    registerShuffleServers(appId, shuffleId, response.getServerToPartitionRanges());
    startHeartbeat();

    LOG.info("RegisterShuffle with ShuffleId[" + shuffleId + "], partitionNum[" + partitionToServers.size() + "]");
    return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, partitionToServers);
  }

  private void startHeartbeat() {
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false) && !heartbeatStarted) {
      scheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              shuffleWriteClient.sendAppHeartbeat(appId, heartbeatTimeout);
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

  @VisibleForTesting
  protected void registerShuffleServers(String appId, int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId[" + shuffleId + "]");
    long start = System.currentTimeMillis();
    serverToPartitionRanges.entrySet()
        .parallelStream()
        .forEach(entry -> {
          shuffleWriteClient.registerShuffle(
              entry.getKey(), appId, shuffleId, entry.getValue());
        });
    LOG.info("Finish register shuffleId[" + shuffleId + "] with " + (System.currentTimeMillis() - start) + " ms");
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String coordinators = sparkConf.get(RssClientConfig.RSS_COORDINATOR_QUORUM);
    LOG.info("Registering coordinators {}", coordinators);
    shuffleWriteClient.registerCoordinators(coordinators);
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId,
      TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle rssHandle = (RssShuffleHandle) handle;
      appId = rssHandle.getAppId();
      int executorId = Integer.MAX_VALUE;
      if (!isDriver) {
        executorId = Integer.parseInt(SparkEnv.get().executorId());
      }

      int shuffleId = rssHandle.getShuffleId();
      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
      ShuffleWriteMetrics writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      WriteBufferManager bufferManager = new WriteBufferManager(
          shuffleId, context.taskAttemptId(), bufferOptions, rssHandle.getDependency().serializer(),
          rssHandle.getPartitionToServers(), context.taskMemoryManager(),
          writeMetrics);
      taskToBuffManager.put(taskId, bufferManager);

      return new RssShuffleWriter(rssHandle.getAppId(), shuffleId, taskId, context.taskAttemptId(), bufferManager,
          writeMetrics, this, sparkConf, shuffleWriteClient, rssHandle);
    } else {
      throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle,
      int startPartition, int endPartition, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      // spark.rss.base.path is not necessary for every storage type, eg, hdfs need but localfile doesn't
      final String shuffleDataBasePath = sparkConf.get(RssClientConfig.RSS_BASE_PATH, "");
      final String storageType = sparkConf.get(RssClientConfig.RSS_STORAGE_TYPE);
      final int indexReadLimit = sparkConf.getInt(RssClientConfig.RSS_INDEX_READ_LIMIT,
          RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
      RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
      final int partitionNumPerRange = sparkConf.getInt(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
          RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
      final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
      long readBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE,
          RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
      if (readBufferSize > Integer.MAX_VALUE) {
        LOG.warn(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE + " can support 2g as max");
        readBufferSize = Integer.MAX_VALUE;
      }
      int shuffleId = rssShuffleHandle.getShuffleId();
      long start = System.currentTimeMillis();
      Roaring64NavigableMap taskIdBitmap = getExpectedTasks(shuffleId, startPartition, endPartition);
      LOG.info("Get taskId cost " + (System.currentTimeMillis() - start) + " ms, and request expected blockIds from "
          + taskIdBitmap.getLongCardinality() + " tasks for shuffleId[" + shuffleId + "], partitionId["
          + startPartition + "]");
      start = System.currentTimeMillis();
      Map<Integer, List<ShuffleServerInfo>> partitionToServers = rssShuffleHandle.getPartitionToServers();
      Roaring64NavigableMap blockIdBitmap = shuffleWriteClient.getShuffleResult(
          clientType, Sets.newHashSet(partitionToServers.get(startPartition)),
          rssShuffleHandle.getAppId(), shuffleId, startPartition);
      LOG.info("Get shuffle blockId cost " + (System.currentTimeMillis() - start) + " ms, and get "
          + blockIdBitmap.getLongCardinality() + " blockIds for shuffleId[" + shuffleId + "], partitionId["
          + startPartition + "]");

      return new RssShuffleReader<K, C>(startPartition, endPartition, context,
          rssShuffleHandle, shuffleDataBasePath, indexReadLimit,
          RssShuffleUtils.newHadoopConfiguration(sparkConf),
          storageType, (int) readBufferSize, partitionNumPerRange, partitionNum,
          blockIdBitmap, taskIdBitmap);
    } else {
      throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle, int startPartition,
      int endPartition, TaskContext context, int startMapId, int endMapId) {
    return null;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return true;
  }

  @Override
  public void stop() {
    scheduledExecutorService.shutdownNow();
    threadPoolExecutor.shutdownNow();
    shuffleWriteClient.close();
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RuntimeException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  public EventLoop getEventLoop() {
    return eventLoop;
  }

  @VisibleForTesting
  public void setEventLoop(EventLoop<AddBlockEvent> eventLoop) {
    this.eventLoop = eventLoop;
  }

  // when speculation enable, duplicate data will be sent and reported to shuffle server,
  // get the actual tasks and filter the duplicate data caused by speculation task
  private Roaring64NavigableMap getExpectedTasks(int shuffleId, int startPartition, int endPartition) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    // In 2.3, getMapSizesByExecutorId returns Seq, while it returns Iterator in 2.4,
    // so we use toIterator() to supoort Spark 2.3 & 2.4
    Iterator<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapStatusIter =
        SparkEnv.get().mapOutputTracker().getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
            .toIterator();
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> tuple2 = mapStatusIter.next();
      Option<String> topologyInfo = tuple2._1().topologyInfo();
      if (topologyInfo.isDefined()) {
        taskIdBitmap.addLong(Long.parseLong(tuple2._1().topologyInfo().get()));
      } else {
        throw new RuntimeException("Can't get expected taskAttemptId");
      }
    }
    LOG.info("Got result from MapStatus for expected tasks " + taskIdBitmap.getLongCardinality());
    return taskIdBitmap;
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

  @VisibleForTesting
  public void addFailedBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToFailedBlockIds.get(taskId) == null) {
      taskToFailedBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToFailedBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void addSuccessBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToSuccessBlockIds.get(taskId) == null) {
      taskToSuccessBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToSuccessBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void clearCachedBlockIds() {
    taskToSuccessBlockIds.clear();
    taskToFailedBlockIds.clear();
  }
}
