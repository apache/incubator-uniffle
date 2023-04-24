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

package org.apache.spark.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.shuffle.writer.DataPusher;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.shuffle.writer.WriteBufferManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.ShuffleManagerGrpcService;
import org.apache.uniffle.shuffle.manager.ShuffleManagerServerFactory;

import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;

public class RssShuffleManager extends RssShuffleManagerBase {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final long heartbeatInterval;
  private final long heartbeatTimeout;
  private ScheduledExecutorService heartBeatScheduledExecutorService;
  private SparkConf sparkConf;
  private String appId = "";
  private String clientType;
  private ShuffleWriteClient shuffleWriteClient;
  private Map<String, Set<Long>> taskToSuccessBlockIds = JavaUtils.newConcurrentMap();
  private Map<String, Set<Long>> taskToFailedBlockIds = JavaUtils.newConcurrentMap();
  private final int dataReplica;
  private final int dataReplicaWrite;
  private final int dataReplicaRead;
  private final boolean dataReplicaSkipEnabled;
  private final int dataTransferPoolSize;
  private final int dataCommitPoolSize;
  private Set<String> failedTaskIds = Sets.newConcurrentHashSet();
  private boolean heartbeatStarted = false;
  private boolean dynamicConfEnabled = false;
  private final String user;
  private final String uuid;
  private DataPusher dataPusher;

  private final Map<Integer, Integer> shuffleIdToPartitionNum = Maps.newConcurrentMap();
  private final Map<Integer, Integer> shuffleIdToNumMapTasks = Maps.newConcurrentMap();
  private GrpcServer shuffleManagerServer;
  private ShuffleManagerGrpcService service;

  public RssShuffleManager(SparkConf sparkConf, boolean isDriver) {
    if (sparkConf.getBoolean("spark.sql.adaptive.enabled", false)) {
      throw new IllegalArgumentException("Spark2 doesn't support AQE, spark.sql.adaptive.enabled should be false.");
    }
    this.sparkConf = sparkConf;
    this.user = sparkConf.get("spark.rss.quota.user", "user");
    this.uuid = sparkConf.get("spark.rss.quota.uuid",  Long.toString(System.currentTimeMillis()));
    // set & check replica config
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    LOG.info("Check quorum config ["
        + dataReplica + ":" + dataReplicaWrite + ":" + dataReplicaRead + ":" + dataReplicaSkipEnabled + "]");
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout = sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    this.dynamicConfEnabled = sparkConf.get(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);
    int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    int unregisterThreadPoolSize = sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterRequestTimeoutSec = sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    this.shuffleWriteClient = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax, heartBeatThreadNum,
            dataReplica, dataReplicaWrite, dataReplicaRead, dataReplicaSkipEnabled, dataTransferPoolSize,
            dataCommitPoolSize, unregisterThreadPoolSize, unregisterRequestTimeoutSec, rssConf);
    registerCoordinator();
    // fetch client conf and apply them if necessary and disable ESS
    if (isDriver && dynamicConfEnabled) {
      Map<String, String> clusterClientConf = shuffleWriteClient.fetchClientConf(
          sparkConf.get(RssSparkConfig.RSS_ACCESS_TIMEOUT_MS));
      RssSparkShuffleUtils.applyDynamicClientConf(sparkConf, clusterClientConf);
    }
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);
    // External shuffle service is not supported when using remote shuffle service
    sparkConf.set("spark.shuffle.service.enabled", "false");
    LOG.info("Disable external shuffle service in RssShuffleManager.");
    // If we store shuffle data in distributed filesystem or in a disaggregated
    // shuffle cluster, we don't need shuffle data locality
    sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
    LOG.info("Disable shuffle data locality in RssShuffleManager.");
    if (!sparkConf.getBoolean(RssSparkConfig.RSS_TEST_FLAG.key(), false)) {
      if (isDriver) {
        heartBeatScheduledExecutorService =
            ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
        if (rssConf.getBoolean(RssClientConfig.RSS_RESUBMIT_STAGE, false)
                && RssSparkShuffleUtils.isStageResubmitSupported()) {
          LOG.info("stage resubmit is supported and enabled");
          // start shuffle manager server
          rssConf.set(RPC_SERVER_PORT, 0);
          ShuffleManagerServerFactory factory = new ShuffleManagerServerFactory(this, rssConf);
          service = factory.getService();
          shuffleManagerServer = factory.getServer(service);
          try {
            shuffleManagerServer.start();
            // pass this as a spark.rss.shuffle.manager.grpc.port config, so it can be propagated to executor properly.
            sparkConf.set(RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT, shuffleManagerServer.getPort());
          } catch (Exception e) {
            LOG.error("Failed to start shuffle manager server", e);
            throw new RssException(e);
          }
        }
      }
      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data pusher is starting...");
      int poolSize = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE);
      int keepAliveTime = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE);
      this.dataPusher = new DataPusher(
          shuffleWriteClient,
          taskToSuccessBlockIds,
          taskToFailedBlockIds,
          failedTaskIds,
          poolSize,
          keepAliveTime
      );
    }
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader).
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {

    //Spark have three kinds of serializer:
    //org.apache.spark.serializer.JavaSerializer
    //org.apache.spark.sql.execution.UnsafeRowSerializer
    //org.apache.spark.serializer.KryoSerializer,
    //Only org.apache.spark.serializer.JavaSerializer don't support RelocationOfSerializedObjects.
    //So when we find the parameters to use org.apache.spark.serializer.JavaSerializer, We should throw an exception
    if (!SparkEnv.get().serializer().supportsRelocationOfSerializedObjects()) {
      throw new IllegalArgumentException("Can't use serialized shuffle for shuffleId: " + shuffleId + ", because the"
              + " serializer: " + SparkEnv.get().serializer().getClass().getName() + " does not support object "
              + "relocation.");
    }

    // If yarn enable retry ApplicationMaster, appId will be not unique and shuffle data will be incorrect,
    // appId + uuid can avoid such problem,
    // can't get appId in construct because SparkEnv is not created yet,
    // appId will be initialized only once in this method which
    // will be called many times depend on how many shuffle stage
    if ("".equals(appId)) {
      appId = SparkEnv.get().conf().getAppId() + "_" + uuid;
      dataPusher.setRssAppId(appId);
      LOG.info("Generate application id used in rss: " + appId);
    }

    if (dependency.partitioner().numPartitions() == 0) {
      shuffleIdToPartitionNum.putIfAbsent(shuffleId, 0);
      shuffleIdToNumMapTasks.putIfAbsent(shuffleId, dependency.rdd().partitions().length);
      LOG.info("RegisterShuffle with ShuffleId[" + shuffleId + "], partitionNum is 0, "
          + "return the empty RssShuffleHandle directly");
      Broadcast<ShuffleHandleInfo> hdlInfoBd = RssSparkShuffleUtils.broadcastShuffleHdlInfo(
          RssSparkShuffleUtils.getActiveSparkContext(), shuffleId, Collections.emptyMap(),
          RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
      return new RssShuffleHandle<>(shuffleId,
        appId,
        dependency.rdd().getNumPartitions(),
        dependency,
        hdlInfoBd);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = new RemoteStorageInfo(
        sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""));
    RemoteStorageInfo remoteStorage = ClientUtils.fetchRemoteStorage(
        appId, defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);

    int partitionNumPerRange = sparkConf.get(RssSparkConfig.RSS_PARTITION_NUM_PER_RANGE);

    // get all register info according to coordinator's response
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    int requiredShuffleServerNumber = RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);

    // retryInterval must bigger than `rss.server.heartbeat.timeout`, or maybe it will return the same result
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers;
    try {
      partitionToServers = RetryUtils.retry(() -> {
        ShuffleAssignmentsInfo response = shuffleWriteClient.getShuffleAssignments(
                appId, shuffleId, dependency.partitioner().numPartitions(),
                partitionNumPerRange, assignmentTags, requiredShuffleServerNumber, -1);
        registerShuffleServers(appId, shuffleId, response.getServerToPartitionRanges(), remoteStorage);
        return response.getPartitionToServers();
      }, retryInterval, retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("registerShuffle failed!", throwable);
    }

    startHeartbeat();

    shuffleIdToPartitionNum.putIfAbsent(shuffleId, dependency.partitioner().numPartitions());
    shuffleIdToNumMapTasks.putIfAbsent(shuffleId, dependency.rdd().partitions().length);
    Broadcast<ShuffleHandleInfo> hdlInfoBd = RssSparkShuffleUtils.broadcastShuffleHdlInfo(
        RssSparkShuffleUtils.getActiveSparkContext(), shuffleId, partitionToServers, remoteStorage);
    LOG.info("RegisterShuffle with ShuffleId[" + shuffleId + "], partitionNum[" + partitionToServers.size() + "]");
    return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, hdlInfoBd);
  }

  private void startHeartbeat() {
    shuffleWriteClient.registerApplicationInfo(appId, heartbeatTimeout, user);
    if (!sparkConf.getBoolean(RssSparkConfig.RSS_TEST_FLAG.key(), false) && !heartbeatStarted) {
      heartBeatScheduledExecutorService.scheduleAtFixedRate(
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
  protected void registerShuffleServers(
      String appId,
      int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      RemoteStorageInfo remoteStorage) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId[" + shuffleId + "]");
    long start = System.currentTimeMillis();
    serverToPartitionRanges.entrySet()
        .stream()
        .forEach(entry -> {
          shuffleWriteClient.registerShuffle(
              entry.getKey(),
              appId,
              shuffleId,
              entry.getValue(),
              remoteStorage,
              ShuffleDataDistributionType.NORMAL
          );
        });
    LOG.info("Finish register shuffleId[" + shuffleId + "] with " + (System.currentTimeMillis() - start) + " ms");
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM.key());
    LOG.info("Registering coordinators {}", coordinators);
    shuffleWriteClient.registerCoordinators(coordinators);
  }

  public CompletableFuture<Long> sendData(AddBlockEvent event) {
    if (dataPusher != null && event != null) {
      return dataPusher.send(event);
    }
    return new CompletableFuture<>();
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId,
      TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle<K, V, ?> rssHandle = (RssShuffleHandle<K, V, ?>) handle;
      appId = rssHandle.getAppId();
      dataPusher.setRssAppId(appId);

      int shuffleId = rssHandle.getShuffleId();
      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
      ShuffleWriteMetrics writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      WriteBufferManager bufferManager = new WriteBufferManager(
          shuffleId,
          taskId,
          context.taskAttemptId(),
          bufferOptions,
          rssHandle.getDependency().serializer(),
          rssHandle.getPartitionToServers(),
          context.taskMemoryManager(),
          writeMetrics,
          RssSparkConfig.toRssConf(sparkConf),
          this::sendData
      );

      return new RssShuffleWriter<>(rssHandle.getAppId(), shuffleId, taskId, context.taskAttemptId(), bufferManager,
          writeMetrics, this, sparkConf, shuffleWriteClient, rssHandle,
          (Function<String, Boolean>) this::markFailedTask);
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle,
      int startPartition, int endPartition, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      final String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
      final int indexReadLimit = sparkConf.get(RssSparkConfig.RSS_INDEX_READ_LIMIT);
      RssShuffleHandle<K, C, ?> rssShuffleHandle = (RssShuffleHandle<K, C, ?>) handle;
      final int partitionNumPerRange = sparkConf.get(RssSparkConfig.RSS_PARTITION_NUM_PER_RANGE);
      final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
      long readBufferSize = sparkConf.getSizeAsBytes(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.key(),
          RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.defaultValue().get());
      if (readBufferSize > Integer.MAX_VALUE) {
        LOG.warn(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE + " can support 2g as max");
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

      final RemoteStorageInfo shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage();
      LOG.info("Shuffle reader using remote storage {}", shuffleRemoteStorageInfo);
      final String shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath();
      Configuration readerHadoopConf = RssSparkShuffleUtils.getRemoteStorageHadoopConf(
          sparkConf, shuffleRemoteStorageInfo);

      return new RssShuffleReader<K, C>(
          startPartition, endPartition, context,
          rssShuffleHandle, shuffleRemoteStoragePath, indexReadLimit,
          readerHadoopConf,
          storageType, (int) readBufferSize, partitionNumPerRange, partitionNum,
          blockIdBitmap, taskIdBitmap, RssSparkConfig.toRssConf(sparkConf));
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle, int startPartition,
      int endPartition, TaskContext context, int startMapId, int endMapId) {
    return null;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    try {
      if (SparkEnv.get().executorId().equals("driver")) {
        shuffleWriteClient.unregisterShuffle(appId, shuffleId);
        shuffleIdToNumMapTasks.remove(shuffleId);
        shuffleIdToPartitionNum.remove(shuffleId);
        if (service != null) {
          service.unregisterShuffle(shuffleId);
        }
      }
    } catch (Exception e) {
      LOG.warn("Errors on unregister to remote shuffle-servers", e);
    }
    return true;
  }

  @Override
  public void stop() {
    if (heartBeatScheduledExecutorService != null) {
      heartBeatScheduledExecutorService.shutdownNow();
    }
    if (dataPusher != null) {
      try {
        dataPusher.close();
      } catch (IOException e) {
        LOG.warn("Errors on closing data pusher", e);
      }
    }
    shuffleWriteClient.close();
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RssException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  // when speculation enable, duplicate data will be sent and reported to shuffle server,
  // get the actual tasks and filter the duplicate data caused by speculation task
  private Roaring64NavigableMap getExpectedTasks(int shuffleId, int startPartition, int endPartition) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    // In 2.3, getMapSizesByExecutorId returns Seq, while it returns Iterator in 2.4,
    // so we use toIterator() to support Spark 2.3 & 2.4
    Iterator<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapStatusIter =
        SparkEnv.get().mapOutputTracker().getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
            .toIterator();
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> tuple2 = mapStatusIter.next();
      Option<String> topologyInfo = tuple2._1().topologyInfo();
      if (topologyInfo.isDefined()) {
        taskIdBitmap.addLong(Long.parseLong(tuple2._1().topologyInfo().get()));
      } else {
        throw new RssException("Can't get expected taskAttemptId");
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

  public void clearTaskMeta(String taskId) {
    taskToSuccessBlockIds.remove(taskId);
    taskToFailedBlockIds.remove(taskId);
  }

  @VisibleForTesting
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  @VisibleForTesting
  public void setAppId(String appId) {
    this.appId = appId;
  }

  public boolean markFailedTask(String taskId) {
    LOG.info("Mark the task: {} failed.", taskId);
    failedTaskIds.add(taskId);
    return true;
  }

  public boolean isValidTask(String taskId) {
    return !failedTaskIds.contains(taskId);
  }

  public DataPusher getDataPusher() {
    return dataPusher;
  }

  public void setDataPusher(DataPusher dataPusher) {
    this.dataPusher = dataPusher;
  }

  /**
   * @return the unique spark id for rss shuffle
   */
  @Override
  public String getAppId() {
    return appId;
  }

  /**
   * @return the maximum number of fetch failures per shuffle partition before that shuffle stage should be recomputed
   */
  @Override
  public int getMaxFetchFailures() {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    return Math.max(1, sparkConf.getInt(TASK_MAX_FAILURE, 4) - 1);
  }

  /**
   * @param shuffleId the shuffleId to query
   * @return the num of partitions(a.k.a reduce tasks) for shuffle with shuffle id.
   */
  @Override
  public int getPartitionNum(int shuffleId) {
    return shuffleIdToPartitionNum.getOrDefault(shuffleId, 0);
  }

  /**
   * @param shuffleId the shuffle id to query
   * @return the num of map tasks for current shuffle with shuffle id.
   */
  @Override
  public int getNumMaps(int shuffleId) {
    return shuffleIdToNumMapTasks.getOrDefault(shuffleId, 0);
  }
}
