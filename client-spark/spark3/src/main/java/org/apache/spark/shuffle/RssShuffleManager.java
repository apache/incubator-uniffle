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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.DataPusher;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.response.RssPartitionToShuffleServerResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.ShuffleManagerGrpcService;
import org.apache.uniffle.shuffle.manager.ShuffleManagerServerFactory;

import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;
import static org.apache.uniffle.common.config.RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE;

public class RssShuffleManager extends RssShuffleManagerBase {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final String clientType;
  private final long heartbeatInterval;
  private final long heartbeatTimeout;
  private AtomicReference<String> id = new AtomicReference<>();
  private final int dataReplica;
  private final int dataReplicaWrite;
  private final int dataReplicaRead;
  private final boolean dataReplicaSkipEnabled;
  private final int dataTransferPoolSize;
  private final int dataCommitPoolSize;
  private final Map<String, Set<Long>> taskToSuccessBlockIds;
  private final Map<String, Set<Long>> taskToFailedBlockIds;
  // Record both the block that failed to be sent and the ShuffleServer
  private final Map<String, Map<Long, BlockingQueue<ShuffleServerInfo>>>
      taskToFailedBlockIdsAndServer;
  private ScheduledExecutorService heartBeatScheduledExecutorService;
  private boolean heartbeatStarted = false;
  private boolean dynamicConfEnabled = false;
  private final ShuffleDataDistributionType dataDistributionType;
  private final int maxConcurrencyPerPartitionToWrite;
  private String user;
  private String uuid;
  private Set<String> failedTaskIds = Sets.newConcurrentHashSet();
  private DataPusher dataPusher;

  private final Map<Integer, Integer> shuffleIdToPartitionNum = JavaUtils.newConcurrentMap();
  private final Map<Integer, Integer> shuffleIdToNumMapTasks = JavaUtils.newConcurrentMap();
  private ShuffleManagerGrpcService service;
  private GrpcServer shuffleManagerServer;

  /** used by columnar rss shuffle writer implementation */
  protected SparkConf sparkConf;

  protected ShuffleWriteClient shuffleWriteClient;

  private ShuffleManagerClient shuffleManagerClient;
  /**
   * Mapping between ShuffleId and ShuffleServer list. ShuffleServer list is dynamically allocated.
   * ShuffleServer is not obtained from RssShuffleHandle, but from this mapping.
   */
  private Map<Integer, ShuffleHandleInfo> shuffleIdToShuffleHandleInfo;
  /** Whether to enable the dynamic shuffleServer function rewrite and reread functions */
  private boolean rssResubmitStage;
  /** A list of shuffleServer for Write failures */
  private Set<String> failuresShuffleServerIds;
  /**
   * Prevent multiple tasks from reporting FetchFailed, resulting in multiple ShuffleServer
   * assignments, stageID, Attemptnumber Whether to reassign the combination flag;
   */
  private Map<String, Boolean> serverAssignedInfos;

  public RssShuffleManager(SparkConf conf, boolean isDriver) {
    this.sparkConf = conf;
    boolean supportsRelocation =
        Optional.ofNullable(SparkEnv.get())
            .map(env -> env.serializer().supportsRelocationOfSerializedObjects())
            .orElse(true);
    if (!supportsRelocation) {
      LOG.warn(
          "RSSShuffleManager requires a serializer which supports relocations of serialized object. Please set "
              + "spark.serializer to org.apache.spark.serializer.KryoSerializer instead");
    }
    this.user = sparkConf.get("spark.rss.quota.user", "user");
    this.uuid = sparkConf.get("spark.rss.quota.uuid", Long.toString(System.currentTimeMillis()));
    // set & check replica config
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    LOG.info(
        "Check quorum config ["
            + dataReplica
            + ":"
            + dataReplicaWrite
            + ":"
            + dataReplicaRead
            + ":"
            + dataReplicaSkipEnabled
            + "]");
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout =
        sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    final int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    this.dynamicConfEnabled = sparkConf.get(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);
    this.dataDistributionType = getDataDistributionType(sparkConf);
    this.maxConcurrencyPerPartitionToWrite =
        RssSparkConfig.toRssConf(sparkConf).get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    int unregisterThreadPoolSize =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterRequestTimeoutSec =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    shuffleWriteClient =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(dataReplica)
                    .replicaWrite(dataReplicaWrite)
                    .replicaRead(dataReplicaRead)
                    .replicaSkipEnabled(dataReplicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize)
                    .unregisterThreadPoolSize(unregisterThreadPoolSize)
                    .unregisterRequestTimeSec(unregisterRequestTimeoutSec)
                    .rssConf(rssConf));
    registerCoordinator();
    // fetch client conf and apply them if necessary and disable ESS
    if (isDriver && dynamicConfEnabled) {
      Map<String, String> clusterClientConf =
          shuffleWriteClient.fetchClientConf(
              sparkConf.getInt(
                  RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.key(),
                  RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.defaultValue().get()));
      RssSparkShuffleUtils.applyDynamicClientConf(sparkConf, clusterClientConf);
    }
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);
    // External shuffle service is not supported when using remote shuffle service
    sparkConf.set("spark.shuffle.service.enabled", "false");
    LOG.info("Disable external shuffle service in RssShuffleManager.");
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false");
    LOG.info("Disable local shuffle reader in RssShuffleManager.");
    // If we store shuffle data in distributed filesystem or in a disaggregated
    // shuffle cluster, we don't need shuffle data locality
    sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
    LOG.info("Disable shuffle data locality in RssShuffleManager.");
    taskToSuccessBlockIds = JavaUtils.newConcurrentMap();
    taskToFailedBlockIds = JavaUtils.newConcurrentMap();
    this.taskToFailedBlockIdsAndServer = JavaUtils.newConcurrentMap();
    this.rssResubmitStage =
        rssConf.getBoolean(RssClientConfig.RSS_RESUBMIT_STAGE, false)
            && RssSparkShuffleUtils.isStageResubmitSupported();
    if (isDriver) {
      heartBeatScheduledExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
      if (rssResubmitStage) {
        LOG.info("stage resubmit is supported and enabled");
        // start shuffle manager server
        rssConf.set(RPC_SERVER_PORT, 0);
        ShuffleManagerServerFactory factory = new ShuffleManagerServerFactory(this, rssConf);
        service = factory.getService();
        shuffleManagerServer = factory.getServer(service);
        try {
          shuffleManagerServer.start();
          // pass this as a spark.rss.shuffle.manager.grpc.port config, so it can be propagated to
          // executor properly.
          sparkConf.set(
              RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT, shuffleManagerServer.getPort());
        } catch (Exception e) {
          LOG.error("Failed to start shuffle manager server", e);
          throw new RssException(e);
        }
      }
    }
    LOG.info("Rss data pusher is starting...");
    int poolSize = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE);
    int keepAliveTime = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE);
    this.dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockIds,
            taskToFailedBlockIdsAndServer,
            failedTaskIds,
            poolSize,
            keepAliveTime);
    this.shuffleIdToShuffleHandleInfo = JavaUtils.newConcurrentMap();
    this.failuresShuffleServerIds = Sets.newHashSet();
    this.serverAssignedInfos = JavaUtils.newConcurrentMap();
  }

  public CompletableFuture<Long> sendData(AddBlockEvent event) {
    if (dataPusher != null && event != null) {
      return dataPusher.send(event);
    }
    return new CompletableFuture<>();
  }

  @VisibleForTesting
  protected static ShuffleDataDistributionType getDataDistributionType(SparkConf sparkConf) {
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    if ((boolean) sparkConf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED())
        && !rssConf.containsKey(RssClientConf.DATA_DISTRIBUTION_TYPE.key())) {
      return ShuffleDataDistributionType.LOCAL_ORDER;
    }

    return rssConf.get(RssClientConf.DATA_DISTRIBUTION_TYPE);
  }

  // For testing only
  @VisibleForTesting
  RssShuffleManager(
      SparkConf conf,
      boolean isDriver,
      DataPusher dataPusher,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, Set<Long>> taskToFailedBlockIds,
      Map<String, Map<Long, BlockingQueue<ShuffleServerInfo>>> taskToFailedBlockIdsAndServer) {
    this.sparkConf = conf;
    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    this.dataDistributionType =
        RssSparkConfig.toRssConf(sparkConf).get(RssClientConf.DATA_DISTRIBUTION_TYPE);
    this.maxConcurrencyPerPartitionToWrite =
        RssSparkConfig.toRssConf(sparkConf).get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout =
        sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    LOG.info(
        "Check quorum config ["
            + dataReplica
            + ":"
            + dataReplicaWrite
            + ":"
            + dataReplicaRead
            + ":"
            + dataReplicaSkipEnabled
            + "]");
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    int unregisterThreadPoolSize =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterRequestTimeoutSec =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    shuffleWriteClient =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(dataReplica)
                    .replicaWrite(dataReplicaWrite)
                    .replicaRead(dataReplicaRead)
                    .replicaSkipEnabled(dataReplicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize)
                    .unregisterThreadPoolSize(unregisterThreadPoolSize)
                    .unregisterRequestTimeSec(unregisterRequestTimeoutSec)
                    .rssConf(RssSparkConfig.toRssConf(sparkConf)));
    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.taskToFailedBlockIds = taskToFailedBlockIds;
    this.taskToFailedBlockIdsAndServer = taskToFailedBlockIdsAndServer;
    this.heartBeatScheduledExecutorService = null;
    this.dataPusher = dataPusher;
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader)
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {

    // Spark have three kinds of serializer:
    // org.apache.spark.serializer.JavaSerializer
    // org.apache.spark.sql.execution.UnsafeRowSerializer
    // org.apache.spark.serializer.KryoSerializer,
    // Only org.apache.spark.serializer.JavaSerializer don't support RelocationOfSerializedObjects.
    // So when we find the parameters to use org.apache.spark.serializer.JavaSerializer, We should
    // throw an exception
    if (!SparkEnv.get().serializer().supportsRelocationOfSerializedObjects()) {
      throw new IllegalArgumentException(
          "Can't use serialized shuffle for shuffleId: "
              + shuffleId
              + ", because the"
              + " serializer: "
              + SparkEnv.get().serializer().getClass().getName()
              + " does not support object "
              + "relocation.");
    }

    if (id.get() == null) {
      id.compareAndSet(null, SparkEnv.get().conf().getAppId() + "_" + uuid);
      dataPusher.setRssAppId(id.get());
    }
    LOG.info("Generate application id used in rss: " + id.get());

    if (dependency.partitioner().numPartitions() == 0) {
      shuffleIdToPartitionNum.putIfAbsent(shuffleId, 0);
      shuffleIdToNumMapTasks.putIfAbsent(shuffleId, dependency.rdd().partitions().length);
      LOG.info(
          "RegisterShuffle with ShuffleId["
              + shuffleId
              + "], partitionNum is 0, "
              + "return the empty RssShuffleHandle directly");
      Broadcast<ShuffleHandleInfo> hdlInfoBd =
          RssSparkShuffleUtils.broadcastShuffleHdlInfo(
              RssSparkShuffleUtils.getActiveSparkContext(),
              shuffleId,
              Collections.emptyMap(),
              RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
      return new RssShuffleHandle<>(
          shuffleId, id.get(), dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = getRemoteStorageInfo(sparkConf);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            id.get(), defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);

    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    int requiredShuffleServerNumber =
        RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);

    // retryInterval must bigger than `rss.server.heartbeat.interval`, or maybe it will return the
    // same result
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers;
    try {
      partitionToServers =
          RetryUtils.retry(
              () -> {
                ShuffleAssignmentsInfo response =
                    shuffleWriteClient.getShuffleAssignments(
                        id.get(),
                        shuffleId,
                        dependency.partitioner().numPartitions(),
                        1,
                        assignmentTags,
                        requiredShuffleServerNumber,
                        estimateTaskConcurrency);
                registerShuffleServers(
                    id.get(), shuffleId, response.getServerToPartitionRanges(), remoteStorage);
                return response.getPartitionToServers();
              },
              retryInterval,
              retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("registerShuffle failed!", throwable);
    }
    startHeartbeat();

    shuffleIdToPartitionNum.putIfAbsent(shuffleId, dependency.partitioner().numPartitions());
    shuffleIdToNumMapTasks.putIfAbsent(shuffleId, dependency.rdd().partitions().length);
    if (rssResubmitStage) {
      ShuffleHandleInfo handleInfo =
          new ShuffleHandleInfo(shuffleId, partitionToServers, remoteStorage);
      shuffleIdToShuffleHandleInfo.put(shuffleId, handleInfo);
    }
    Broadcast<ShuffleHandleInfo> hdlInfoBd =
        RssSparkShuffleUtils.broadcastShuffleHdlInfo(
            RssSparkShuffleUtils.getActiveSparkContext(),
            shuffleId,
            partitionToServers,
            remoteStorage);
    LOG.info(
        "RegisterShuffle with ShuffleId["
            + shuffleId
            + "], partitionNum["
            + partitionToServers.size()
            + "], shuffleServerForResult: "
            + partitionToServers);
    return new RssShuffleHandle<>(
        shuffleId, id.get(), dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle<K, V, ?> rssHandle = (RssShuffleHandle<K, V, ?>) handle;
    setPusherAppId(rssHandle);
    int shuffleId = rssHandle.getShuffleId();
    ShuffleWriteMetrics writeMetrics;
    if (metrics != null) {
      writeMetrics = new WriteMetrics(metrics);
    } else {
      writeMetrics = context.taskMetrics().shuffleWriteMetrics();
    }
    ShuffleHandleInfo shuffleHandleInfo;
    if (rssResubmitStage) {
      // Get the ShuffleServer list from the Driver based on the shuffleId
      shuffleHandleInfo = getRemoteShuffleHandleInfo(shuffleId);
    } else {
      shuffleHandleInfo =
          new ShuffleHandleInfo(
              shuffleId, rssHandle.getPartitionToServers(), rssHandle.getRemoteStorage());
    }
    String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    LOG.info("RssHandle appId {} shuffleId {} ", rssHandle.getAppId(), rssHandle.getShuffleId());
    return new RssShuffleWriter<>(
        rssHandle.getAppId(),
        shuffleId,
        taskId,
        context.taskAttemptId(),
        writeMetrics,
        this,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        this::markFailedTask,
        context,
        shuffleHandleInfo);
  }

  public void setPusherAppId(RssShuffleHandle rssShuffleHandle) {
    // todo: this implement is tricky, we should refactor it
    if (id.get() == null) {
      id.compareAndSet(null, rssShuffleHandle.getAppId());
      dataPusher.setRssAppId(id.get());
    }
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return getReader(handle, 0, Integer.MAX_VALUE, startPartition, endPartition, context, metrics);
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
    Roaring64NavigableMap taskIdBitmap =
        getExpectedTasksByExecutorId(
            handle.shuffleId(), startPartition, endPartition, startMapIndex, endMapIndex);
    LOG.info(
        "Get taskId cost "
            + (System.currentTimeMillis() - start)
            + " ms, and request expected blockIds from "
            + taskIdBitmap.getLongCardinality()
            + " tasks for shuffleId["
            + handle.shuffleId()
            + "], partitionId["
            + startPartition
            + ", "
            + endPartition
            + "]");
    return getReaderImpl(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics,
        taskIdBitmap);
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
    Roaring64NavigableMap taskIdBitmap =
        getExpectedTasksByRange(
            handle.shuffleId(), startPartition, endPartition, startMapIndex, endMapIndex);
    LOG.info(
        "Get taskId cost "
            + (System.currentTimeMillis() - start)
            + " ms, and request expected blockIds from "
            + taskIdBitmap.getLongCardinality()
            + " tasks for shuffleId["
            + handle.shuffleId()
            + "], partitionId["
            + startPartition
            + ", "
            + endPartition
            + "]");
    return getReaderImpl(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics,
        taskIdBitmap);
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
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle<K, ?, C> rssShuffleHandle = (RssShuffleHandle<K, ?, C>) handle;
    final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
    int shuffleId = rssShuffleHandle.getShuffleId();
    ShuffleHandleInfo shuffleHandleInfo;
    if (rssResubmitStage) {
      // Get the ShuffleServer list from the Driver based on the shuffleId
      shuffleHandleInfo = getRemoteShuffleHandleInfo(shuffleId);
    } else {
      shuffleHandleInfo =
          new ShuffleHandleInfo(
              shuffleId,
              rssShuffleHandle.getPartitionToServers(),
              rssShuffleHandle.getRemoteStorage());
    }
    Map<Integer, List<ShuffleServerInfo>> allPartitionToServers =
        shuffleHandleInfo.getPartitionToServers();
    Map<Integer, List<ShuffleServerInfo>> requirePartitionToServers =
        allPartitionToServers.entrySet().stream()
            .filter(x -> x.getKey() >= startPartition && x.getKey() < endPartition)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        RssUtils.generateServerToPartitions(requirePartitionToServers);
    long start = System.currentTimeMillis();
    Roaring64NavigableMap blockIdBitmap =
        getShuffleResultForMultiPart(
            clientType,
            serverToPartitions,
            rssShuffleHandle.getAppId(),
            shuffleId,
            context.stageAttemptNumber());
    LOG.info(
        "Get shuffle blockId cost "
            + (System.currentTimeMillis() - start)
            + " ms, and get "
            + blockIdBitmap.getLongCardinality()
            + " blockIds for shuffleId["
            + shuffleId
            + "], startPartition["
            + startPartition
            + "], endPartition["
            + endPartition
            + "]");

    ShuffleReadMetrics readMetrics;
    if (metrics != null) {
      readMetrics = new ReadMetrics(metrics);
    } else {
      readMetrics = context.taskMetrics().shuffleReadMetrics();
    }

    final RemoteStorageInfo shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage();
    LOG.info("Shuffle reader using remote storage {}", shuffleRemoteStorageInfo);
    final String shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath();
    Configuration readerHadoopConf =
        RssSparkShuffleUtils.getRemoteStorageHadoopConf(sparkConf, shuffleRemoteStorageInfo);

    return new RssShuffleReader<K, C>(
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        rssShuffleHandle,
        shuffleRemoteStoragePath,
        readerHadoopConf,
        partitionNum,
        RssUtils.generatePartitionToBitmap(blockIdBitmap, startPartition, endPartition),
        taskIdBitmap,
        readMetrics,
        RssSparkConfig.toRssConf(sparkConf),
        dataDistributionType,
        allPartitionToServers);
  }

  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  private Roaring64NavigableMap getExpectedTasksByExecutorId(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    // Since Spark 3.1 refactors the interface of getMapSizesByExecutorId,
    // we use reflection and catch for the compatibility with 3.0 & 3.1 & 3.2
    try {
      // attempt to use Spark 3.1's API
      mapStatusIter =
          (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
              SparkEnv.get()
                  .mapOutputTracker()
                  .getClass()
                  .getDeclaredMethod(
                      "getMapSizesByExecutorId",
                      int.class,
                      int.class,
                      int.class,
                      int.class,
                      int.class)
                  .invoke(
                      SparkEnv.get().mapOutputTracker(),
                      shuffleId,
                      startMapIndex,
                      endMapIndex,
                      startPartition,
                      endPartition);
    } catch (Exception ignored) {
      // fallback and attempt to use Spark 3.0's API
      try {
        mapStatusIter =
            (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                SparkEnv.get()
                    .mapOutputTracker()
                    .getClass()
                    .getDeclaredMethod("getMapSizesByExecutorId", int.class, int.class, int.class)
                    .invoke(
                        SparkEnv.get().mapOutputTracker(), shuffleId, startPartition, endPartition);
      } catch (Exception ignored1) {
        try {
          // attempt to use Spark 3.2.0's API
          // Each Spark release will be versioned: [MAJOR].[FEATURE].[MAINTENANCE].
          // Usually we only need to adapt [MAJOR].[FEATURE] . Unfortunately,
          // some interfaces were removed wrongly in Spark 3.2.0. And they were added by Spark
          // 3.2.1.
          // So we need to adapt Spark 3.2.0 here
          mapStatusIter =
              (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                  MapOutputTracker.class
                      .getDeclaredMethod(
                          "getMapSizesByExecutorId",
                          int.class,
                          int.class,
                          int.class,
                          int.class,
                          int.class)
                      .invoke(
                          SparkEnv.get().mapOutputTracker(),
                          shuffleId,
                          startMapIndex,
                          endMapIndex,
                          startPartition,
                          endPartition);
        } catch (Exception e) {
          throw new RssException(e);
        }
      }
    }
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> tuple2 = mapStatusIter.next();
      if (!tuple2._1().topologyInfo().isDefined()) {
        throw new RssException("Can't get expected taskAttemptId");
      }
      taskIdBitmap.add(Long.parseLong(tuple2._1().topologyInfo().get()));
    }
    return taskIdBitmap;
  }

  // This API is only used by Spark3.0 and removed since 3.1,
  // so we extract it from getExpectedTasksByExecutorId.
  private Roaring64NavigableMap getExpectedTasksByRange(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    try {
      mapStatusIter =
          (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
              SparkEnv.get()
                  .mapOutputTracker()
                  .getClass()
                  .getDeclaredMethod(
                      "getMapSizesByRange", int.class, int.class, int.class, int.class, int.class)
                  .invoke(
                      SparkEnv.get().mapOutputTracker(),
                      shuffleId,
                      startMapIndex,
                      endMapIndex,
                      startPartition,
                      endPartition);
    } catch (Exception e) {
      throw new RssException(e);
    }
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> tuple2 = mapStatusIter.next();
      if (!tuple2._1().topologyInfo().isDefined()) {
        throw new RssException("Can't get expected taskAttemptId");
      }
      taskIdBitmap.add(Long.parseLong(tuple2._1().topologyInfo().get()));
    }
    return taskIdBitmap;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    try {
      if (SparkEnv.get().executorId().equals("driver")) {
        shuffleWriteClient.unregisterShuffle(id.get(), shuffleId);
        shuffleIdToPartitionNum.remove(shuffleId);
        shuffleIdToNumMapTasks.remove(shuffleId);
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
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RssException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  @Override
  public void stop() {
    if (heartBeatScheduledExecutorService != null) {
      heartBeatScheduledExecutorService.shutdownNow();
    }
    if (shuffleWriteClient != null) {
      // Unregister shuffle before closing shuffle write client.
      shuffleWriteClient.unregisterShuffle(getAppId());
      shuffleWriteClient.close();
    }
    if (dataPusher != null) {
      try {
        dataPusher.close();
      } catch (IOException e) {
        LOG.warn("Errors on closing data pusher", e);
      }
    }

    if (shuffleManagerServer != null) {
      try {
        shuffleManagerServer.stop();
      } catch (InterruptedException e) {
        // ignore
        LOG.info("shuffle manager server is interrupted during stop");
      }
    }
  }

  public void clearTaskMeta(String taskId) {
    taskToSuccessBlockIds.remove(taskId);
    taskToFailedBlockIds.remove(taskId);
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
    Set<Map.Entry<ShuffleServerInfo, List<PartitionRange>>> entries =
        serverToPartitionRanges.entrySet();
    entries.stream()
        .forEach(
            entry -> {
              shuffleWriteClient.registerShuffle(
                  entry.getKey(),
                  appId,
                  shuffleId,
                  entry.getValue(),
                  remoteStorage,
                  dataDistributionType,
                  maxConcurrencyPerPartitionToWrite);
            });
    LOG.info(
        "Finish register shuffleId["
            + shuffleId
            + "] with "
            + (System.currentTimeMillis() - start)
            + " ms");
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM.key());
    LOG.info("Start Registering coordinators {}", coordinators);
    shuffleWriteClient.registerCoordinators(coordinators);
  }

  @VisibleForTesting
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  private synchronized void startHeartbeat() {
    shuffleWriteClient.registerApplicationInfo(id.get(), heartbeatTimeout, user);
    if (!heartbeatStarted) {
      heartBeatScheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              String appId = id.get();
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

  public Set<Long> getFailedBlockIds(String taskId) {
    Set<Long> result = taskToFailedBlockIds.get(taskId);
    if (result == null) {
      result = Collections.emptySet();
    }
    return result;
  }

  public Set<Long> getSuccessBlockIds(String taskId) {
    Set<Long> result = taskToSuccessBlockIds.get(taskId);
    if (result == null) {
      result = Collections.emptySet();
    }
    return result;
  }

  /** @return the unique spark id for rss shuffle */
  @Override
  public String getAppId() {
    return id.get();
  }

  /**
   * @return the maximum number of fetch failures per shuffle partition before that shuffle stage
   *     should be recomputed
   */
  @Override
  public int getMaxFetchFailures() {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    return Math.max(1, sparkConf.getInt(TASK_MAX_FAILURE, 4) - 1);
  }

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

  static class ReadMetrics extends ShuffleReadMetrics {
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

  public static class WriteMetrics extends ShuffleWriteMetrics {
    private ShuffleWriteMetricsReporter reporter;

    public WriteMetrics(ShuffleWriteMetricsReporter reporter) {
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

  @VisibleForTesting
  public void setAppId(String appId) {
    this.id = new AtomicReference<>(appId);
  }

  public boolean markFailedTask(String taskId) {
    LOG.info("Mark the task: {} failed.", taskId);
    failedTaskIds.add(taskId);
    return true;
  }

  public boolean isValidTask(String taskId) {
    return !failedTaskIds.contains(taskId);
  }

  private Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      int stageAttemptId) {
    Set<Integer> failedPartitions = Sets.newHashSet();
    try {
      return shuffleWriteClient.getShuffleResultForMultiPart(
          clientType, serverToPartitions, appId, shuffleId, failedPartitions);
    } catch (RssFetchFailedException e) {
      throw RssSparkShuffleUtils.reportRssFetchFailedException(
          e, sparkConf, appId, shuffleId, stageAttemptId, failedPartitions);
    }
  }

  /**
   * The ShuffleServer list of block sending failures is returned using the shuffle task ID
   *
   * @param taskId Shuffle taskId
   * @return failed ShuffleServer blocks
   */
  public Map<Long, BlockingQueue<ShuffleServerInfo>> getFailedBlockIdsWithShuffleServer(
      String taskId) {
    Map<Long, BlockingQueue<ShuffleServerInfo>> result = taskToFailedBlockIdsAndServer.get(taskId);
    if (result == null) {
      result = Collections.emptyMap();
    }
    return result;
  }

  @Override
  public ShuffleHandleInfo getShuffleHandleInfoByShuffleId(int shuffleId) {
    return shuffleIdToShuffleHandleInfo.get(shuffleId);
  }

  private ShuffleManagerClient createShuffleManagerClient(String host, int port) {
    // Host can be inferred from `spark.driver.bindAddress`, which would be set when SparkContext is
    // constructed.
    return ShuffleManagerClientFactory.getInstance()
        .createShuffleManagerClient(ClientType.GRPC, host, port);
  }

  /**
   * Get the ShuffleServer list from the Driver based on the shuffleId
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  private synchronized ShuffleHandleInfo getRemoteShuffleHandleInfo(int shuffleId) {
    ShuffleHandleInfo shuffleHandleInfo;
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    String driver = rssConf.getString("driver.host", "");
    int port = rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT);
    if (shuffleManagerClient == null) {
      shuffleManagerClient = createShuffleManagerClient(driver, port);
    }
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(shuffleId);
    RssPartitionToShuffleServerResponse rpcPartitionToShufflerServer =
        shuffleManagerClient.getPartitionToShufflerServer(rssPartitionToShuffleServerRequest);
    shuffleHandleInfo =
        new ShuffleHandleInfo(
            shuffleId,
            rpcPartitionToShufflerServer.getPartitionToServers(),
            rpcPartitionToShufflerServer.getRemoteStorageInfo());
    return shuffleHandleInfo;
  }

  /**
   * Add the shuffleServer that failed to write to the failure list
   *
   * @param shuffleServerId
   */
  @Override
  public void addFailuresShuffleServerInfos(String shuffleServerId) {
    failuresShuffleServerIds.add(shuffleServerId);
  }

  /**
   * Reassign the ShuffleServer list for ShuffleId
   *
   * @param shuffleId
   * @param numPartitions
   */
  @Override
  public synchronized boolean reassignShuffleServers(
      int stageId, int stageAttemptNumber, int shuffleId, int numPartitions) {
    String stageIdAndAttempt = stageId + "_" + stageAttemptNumber;
    Boolean needReassgin = serverAssignedInfos.computeIfAbsent(stageIdAndAttempt, id -> false);
    if (!needReassgin) {
      String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
      RemoteStorageInfo defaultRemoteStorage =
          new RemoteStorageInfo(sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""));
      RemoteStorageInfo remoteStorage =
          ClientUtils.fetchRemoteStorage(
              id.get(), defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);
      Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
      int requiredShuffleServerNumber =
          RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
      long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
      int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
      int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
      /** Before reassigning ShuffleServer, clear the ShuffleServer list in ShuffleWriteClient. */
      shuffleWriteClient.unregisterShuffle(id.get(), shuffleId);
      Map<Integer, List<ShuffleServerInfo>> partitionToServers;
      try {
        partitionToServers =
            RetryUtils.retry(
                () -> {
                  ShuffleAssignmentsInfo response =
                      shuffleWriteClient.getShuffleAssignments(
                          id.get(),
                          shuffleId,
                          numPartitions,
                          1,
                          assignmentTags,
                          requiredShuffleServerNumber,
                          estimateTaskConcurrency,
                          failuresShuffleServerIds);
                  registerShuffleServers(
                      id.get(), shuffleId, response.getServerToPartitionRanges(), remoteStorage);
                  return response.getPartitionToServers();
                },
                retryInterval,
                retryTimes);

      } catch (Throwable throwable) {
        throw new RssException("registerShuffle failed!", throwable);
      }
      /**
       * we need to clear the metadata of the completed task, otherwise some of the stage's data
       * will be lost
       */
      try {
        unregisterAllMapOutput(shuffleId);
      } catch (SparkException e) {
        LOG.error("Clear MapoutTracker Meta failed!");
        throw new RssException("Clear MapoutTracker Meta failed!", e);
      }
      ShuffleHandleInfo handleInfo =
          new ShuffleHandleInfo(shuffleId, partitionToServers, remoteStorage);
      shuffleIdToShuffleHandleInfo.put(shuffleId, handleInfo);
      serverAssignedInfos.put(stageIdAndAttempt, true);
      return true;
    } else {
      LOG.info(
          "The Stage:{} has been reassigned in an Attempt{},Return without performing any operation",
          stageId,
          stageAttemptNumber);
      return false;
    }
  }

  public boolean isRssResubmitStage() {
    return rssResubmitStage;
  }
}
