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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections4.CollectionUtils;
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
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
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

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.response.RssPartitionToShuffleServerResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.RssShuffleClientFactory;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.ShuffleManagerGrpcService;
import org.apache.uniffle.shuffle.manager.ShuffleManagerServerFactory;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM;
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
  private final Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker;
  private ScheduledExecutorService heartBeatScheduledExecutorService;
  private boolean heartbeatStarted = false;
  private boolean dynamicConfEnabled;
  private final ShuffleDataDistributionType dataDistributionType;
  private final BlockIdLayout blockIdLayout;
  private final int maxConcurrencyPerPartitionToWrite;
  private final int maxFailures;
  private final boolean speculation;
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
  /** Whether to enable the dynamic shuffleServer function rewrite and reread functions */
  private boolean rssResubmitStage;

  private boolean taskBlockSendFailureRetryEnabled;

  private boolean shuffleManagerRpcServiceEnabled;
  /** A list of shuffleServer for Write failures */
  private Set<String> failuresShuffleServerIds;
  /**
   * Prevent multiple tasks from reporting FetchFailed, resulting in multiple ShuffleServer
   * assignments, stageID, Attemptnumber Whether to reassign the combination flag;
   */
  private Map<String, Boolean> serverAssignedInfos;

  private final int partitionReassignMaxServerNum;

  private final ShuffleHandleInfoManager shuffleHandleInfoManager = new ShuffleHandleInfoManager();
  private boolean blockIdSelfManagedEnabled;

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
    this.dynamicConfEnabled = sparkConf.get(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);

    // fetch client conf and apply them if necessary
    if (isDriver && this.dynamicConfEnabled) {
      fetchAndApplyDynamicConf(sparkConf);
    }
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);

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
    this.dataDistributionType = getDataDistributionType(sparkConf);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    this.maxConcurrencyPerPartitionToWrite = rssConf.get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    this.maxFailures = sparkConf.getInt("spark.task.maxFailures", 4);
    this.speculation = sparkConf.getBoolean("spark.speculation", false);
    // configureBlockIdLayout requires maxFailures and speculation to be initialized
    configureBlockIdLayout(sparkConf, rssConf);
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    // External shuffle service is not supported when using remote shuffle service
    sparkConf.set("spark.shuffle.service.enabled", "false");
    sparkConf.set("spark.dynamicAllocation.shuffleTracking.enabled", "false");
    LOG.info("Disable external shuffle service in RssShuffleManager.");
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false");
    LOG.info("Disable local shuffle reader in RssShuffleManager.");
    // If we store shuffle data in distributed filesystem or in a disaggregated
    // shuffle cluster, we don't need shuffle data locality
    sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
    LOG.info("Disable shuffle data locality in RssShuffleManager.");
    taskToSuccessBlockIds = JavaUtils.newConcurrentMap();
    taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    this.rssResubmitStage =
        rssConf.getBoolean(RssClientConfig.RSS_RESUBMIT_STAGE, false)
            && RssSparkShuffleUtils.isStageResubmitSupported();
    this.taskBlockSendFailureRetryEnabled =
        rssConf.getBoolean(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED);

    // The feature of partition reassign is exclusive with multiple replicas and stage retry.
    if (taskBlockSendFailureRetryEnabled) {
      if (rssResubmitStage || dataReplica > 1) {
        throw new RssException(
            "The feature of partition reassign is incompatible with multiple replicas and stage retry.");
      }
    }

    this.blockIdSelfManagedEnabled = rssConf.getBoolean(RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED);
    this.shuffleManagerRpcServiceEnabled =
        taskBlockSendFailureRetryEnabled || rssResubmitStage || blockIdSelfManagedEnabled;
    if (isDriver) {
      heartBeatScheduledExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
      if (shuffleManagerRpcServiceEnabled) {
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
    if (shuffleManagerRpcServiceEnabled) {
      this.shuffleManagerClient = getOrCreateShuffleManagerClient();
    }
    int unregisterThreadPoolSize =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterRequestTimeoutSec =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    shuffleWriteClient =
        RssShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                RssShuffleClientFactory.newWriteBuilder()
                    .blockIdSelfManagedEnabled(blockIdSelfManagedEnabled)
                    .shuffleManagerClient(shuffleManagerClient)
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

    LOG.info("Rss data pusher is starting...");
    int poolSize = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE);
    int keepAliveTime = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE);
    this.dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            poolSize,
            keepAliveTime);
    this.failuresShuffleServerIds = Sets.newHashSet();
    this.serverAssignedInfos = JavaUtils.newConcurrentMap();
    this.partitionReassignMaxServerNum =
        rssConf.get(RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM);
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
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker) {
    this.sparkConf = conf;
    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    this.dataDistributionType = rssConf.get(RssClientConf.DATA_DISTRIBUTION_TYPE);
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.maxConcurrencyPerPartitionToWrite = rssConf.get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    this.maxFailures = sparkConf.getInt("spark.task.maxFailures", 4);
    this.speculation = sparkConf.getBoolean("spark.speculation", false);
    // configureBlockIdLayout requires maxFailures and speculation to be initialized
    configureBlockIdLayout(sparkConf, rssConf);
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
        RssShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                RssShuffleClientFactory.getInstance()
                    .newWriteBuilder()
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
    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.heartBeatScheduledExecutorService = null;
    this.taskToFailedBlockSendTracker = taskToFailedBlockSendTracker;
    this.dataPusher = dataPusher;
    this.partitionReassignMaxServerNum =
        rssConf.get(RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM);
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader)
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {

    // fail fast if number of partitions is not supported by block id layout
    if (dependency.partitioner().numPartitions() > blockIdLayout.maxNumPartitions) {
      throw new RssException(
          "Cannot register shuffle with "
              + dependency.partitioner().numPartitions()
              + " partitions because the configured block id layout supports at most "
              + blockIdLayout.maxNumPartitions
              + " partitions.");
    }

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
      Broadcast<SimpleShuffleHandleInfo> hdlInfoBd =
          RssSparkShuffleUtils.broadcastShuffleHdlInfo(
              RssSparkShuffleUtils.getActiveSparkContext(),
              shuffleId,
              Collections.emptyMap(),
              RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
      return new RssShuffleHandle<>(
          shuffleId, id.get(), dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = getDefaultRemoteStorageInfo(sparkConf);
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
    if (shuffleManagerRpcServiceEnabled) {
      MutableShuffleHandleInfo handleInfo =
          new MutableShuffleHandleInfo(shuffleId, partitionToServers, remoteStorage);
      shuffleHandleInfoManager.register(shuffleId, handleInfo);
    }
    Broadcast<SimpleShuffleHandleInfo> hdlInfoBd =
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
    if (shuffleManagerRpcServiceEnabled) {
      // Get the ShuffleServer list from the Driver based on the shuffleId
      shuffleHandleInfo = getRemoteShuffleHandleInfo(shuffleId);
    } else {
      shuffleHandleInfo =
          new SimpleShuffleHandleInfo(
              shuffleId, rssHandle.getPartitionToServers(), rssHandle.getRemoteStorage());
    }
    String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    LOG.info("RssHandle appId {} shuffleId {} ", rssHandle.getAppId(), rssHandle.getShuffleId());
    return new RssShuffleWriter<>(
        rssHandle.getAppId(),
        shuffleId,
        taskId,
        getTaskAttemptIdForBlockId(context.partitionId(), context.attemptNumber()),
        writeMetrics,
        this,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        this::markFailedTask,
        context,
        shuffleHandleInfo);
  }

  @Override
  public void configureBlockIdLayout(SparkConf sparkConf, RssConf rssConf) {
    configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
  }

  @Override
  public long getTaskAttemptIdForBlockId(int mapIndex, int attemptNo) {
    return getTaskAttemptIdForBlockId(
        mapIndex, attemptNo, maxFailures, speculation, blockIdLayout.taskAttemptIdBits);
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
    if (shuffleManagerRpcServiceEnabled) {
      // Get the ShuffleServer list from the Driver based on the shuffleId
      shuffleHandleInfo = getRemoteShuffleHandleInfo(shuffleId);
    } else {
      shuffleHandleInfo =
          new SimpleShuffleHandleInfo(
              shuffleId,
              rssShuffleHandle.getPartitionToServers(),
              rssShuffleHandle.getRemoteStorage());
    }
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        getPartitionDataServers(shuffleHandleInfo, startPartition, endPartition);
    long start = System.currentTimeMillis();
    BlockIdSet blockIdBitmap =
        getShuffleResultForMultiPart(
            clientType,
            serverToPartitions,
            rssShuffleHandle.getAppId(),
            shuffleId,
            context.stageAttemptNumber(),
            shuffleHandleInfo.createPartitionReplicaTracking());
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
        RssUtils.generatePartitionToBitmap(
            blockIdBitmap, startPartition, endPartition, blockIdLayout),
        taskIdBitmap,
        readMetrics,
        RssSparkConfig.toRssConf(sparkConf),
        dataDistributionType,
        shuffleHandleInfo.getAllPartitionServersForReader());
  }

  private Map<ShuffleServerInfo, Set<Integer>> getPartitionDataServers(
      ShuffleHandleInfo shuffleHandleInfo, int startPartition, int endPartition) {
    Map<Integer, List<ShuffleServerInfo>> allPartitionToServers =
        shuffleHandleInfo.getAllPartitionServersForReader();
    Map<Integer, List<ShuffleServerInfo>> requirePartitionToServers =
        allPartitionToServers.entrySet().stream()
            .filter(x -> x.getKey() >= startPartition && x.getKey() < endPartition)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        RssUtils.generateServerToPartitions(requirePartitionToServers);
    return serverToPartitions;
  }

  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  private Roaring64NavigableMap getExpectedTasksByExecutorId(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    try {
      // Since Spark 3.1 refactors the interface of getMapSizesByExecutorId,
      // we use reflection and catch for the compatibility with 3.0 & 3.1 & 3.2
      if (Spark3VersionUtils.MAJOR_VERSION > 3
          || Spark3VersionUtils.MINOR_VERSION > 2
          || Spark3VersionUtils.MINOR_VERSION == 2 && !Spark3VersionUtils.isSpark320()
          || Spark3VersionUtils.MINOR_VERSION == 1) {
        // use Spark 3.1's API
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
      } else if (Spark3VersionUtils.isSpark320()) {
        // use Spark 3.2.0's API
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
      } else {
        // use Spark 3.0's API
        mapStatusIter =
            (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                SparkEnv.get()
                    .mapOutputTracker()
                    .getClass()
                    .getDeclaredMethod("getMapSizesByExecutorId", int.class, int.class, int.class)
                    .invoke(
                        SparkEnv.get().mapOutputTracker(), shuffleId, startPartition, endPartition);
      }
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
      super.unregisterShuffle(shuffleId);
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
    taskToFailedBlockSendTracker.remove(taskId);
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
    FailedBlockSendTracker blockIdsFailedSendTracker = getBlockIdsFailedSendTracker(taskId);
    if (blockIdsFailedSendTracker == null) {
      return Collections.emptySet();
    }
    return blockIdsFailedSendTracker.getFailedBlockIds();
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

  private BlockIdSet getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      int stageAttemptId,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    Set<Integer> failedPartitions = Sets.newHashSet();
    try {
      return shuffleWriteClient.getShuffleResultForMultiPart(
          clientType,
          serverToPartitions,
          appId,
          shuffleId,
          failedPartitions,
          replicaRequirementTracking);
    } catch (RssFetchFailedException e) {
      throw RssSparkShuffleUtils.reportRssFetchFailedException(
          e, sparkConf, appId, shuffleId, stageAttemptId, failedPartitions);
    }
  }

  public FailedBlockSendTracker getBlockIdsFailedSendTracker(String taskId) {
    return taskToFailedBlockSendTracker.get(taskId);
  }

  @Override
  public ShuffleHandleInfo getShuffleHandleInfoByShuffleId(int shuffleId) {
    return shuffleHandleInfoManager.get(shuffleId);
  }

  // todo: automatic close client when the client is idle to avoid too much connections for spark
  // driver.
  private ShuffleManagerClient getOrCreateShuffleManagerClient() {
    if (shuffleManagerClient == null) {
      RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
      String driver = rssConf.getString("driver.host", "");
      int port = rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT);
      this.shuffleManagerClient =
          ShuffleManagerClientFactory.getInstance()
              .createShuffleManagerClient(ClientType.GRPC, driver, port);
    }
    return shuffleManagerClient;
  }

  /**
   * Get the ShuffleServer list from the Driver based on the shuffleId
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  private synchronized MutableShuffleHandleInfo getRemoteShuffleHandleInfo(int shuffleId) {
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(shuffleId);
    RssPartitionToShuffleServerResponse rpcPartitionToShufflerServer =
        getOrCreateShuffleManagerClient()
            .getPartitionToShufflerServer(rssPartitionToShuffleServerRequest);
    MutableShuffleHandleInfo shuffleHandleInfo =
        MutableShuffleHandleInfo.fromProto(
            rpcPartitionToShufflerServer.getShuffleHandleInfoProto());
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
  public synchronized boolean reassignAllShuffleServersForWholeStage(
      int stageId, int stageAttemptNumber, int shuffleId, int numPartitions) {
    String stageIdAndAttempt = stageId + "_" + stageAttemptNumber;
    Boolean needReassign = serverAssignedInfos.computeIfAbsent(stageIdAndAttempt, id -> false);
    if (!needReassign) {
      int requiredShuffleServerNumber =
          RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
      int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
      /** Before reassigning ShuffleServer, clear the ShuffleServer list in ShuffleWriteClient. */
      shuffleWriteClient.unregisterShuffle(id.get(), shuffleId);
      Map<Integer, List<ShuffleServerInfo>> partitionToServers =
          requestShuffleAssignment(
              shuffleId,
              numPartitions,
              1,
              requiredShuffleServerNumber,
              estimateTaskConcurrency,
              failuresShuffleServerIds,
              null);
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
      MutableShuffleHandleInfo handleInfo =
          new MutableShuffleHandleInfo(shuffleId, partitionToServers, getRemoteStorageInfo());
      shuffleHandleInfoManager.register(shuffleId, handleInfo);
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

  /** this is only valid on driver side that exposed to being invoked by grpc server */
  @Override
  public MutableShuffleHandleInfo reassignOnBlockSendFailure(
      int shuffleId, Map<Integer, List<ReceivingFailureServer>> partitionToFailureServers) {
    long startTime = System.currentTimeMillis();
    MutableShuffleHandleInfo handleInfo =
        (MutableShuffleHandleInfo) shuffleHandleInfoManager.get(shuffleId);
    synchronized (handleInfo) {
      // If the reassignment servers for one partition exceeds the max reassign server num,
      // it should fast fail.
      handleInfo.checkPartitionReassignServerNum(
          partitionToFailureServers.keySet(), partitionReassignMaxServerNum);

      Map<ShuffleServerInfo, List<PartitionRange>> newServerToPartitions = new HashMap<>();
      // receivingFailureServer -> partitionId -> replacementServerIds. For logging
      Map<String, Map<Integer, Set<String>>> reassignResult = new HashMap<>();

      for (Map.Entry<Integer, List<ReceivingFailureServer>> entry :
          partitionToFailureServers.entrySet()) {
        int partitionId = entry.getKey();
        for (ReceivingFailureServer receivingFailureServer : entry.getValue()) {
          StatusCode code = receivingFailureServer.getStatusCode();
          String serverId = receivingFailureServer.getServerId();

          boolean serverHasReplaced = false;
          Set<ShuffleServerInfo> replacements = handleInfo.getReplacements(serverId);
          if (CollectionUtils.isEmpty(replacements)) {
            final int requiredServerNum = 1;
            Set<String> excludedServers = new HashSet<>(handleInfo.listExcludedServers());
            excludedServers.add(serverId);
            replacements =
                reassignServerForTask(
                    shuffleId, Sets.newHashSet(partitionId), excludedServers, requiredServerNum);
          } else {
            serverHasReplaced = true;
          }

          Set<ShuffleServerInfo> updatedReassignServers =
              handleInfo.updateAssignment(partitionId, serverId, replacements);

          reassignResult
              .computeIfAbsent(serverId, x -> new HashMap<>())
              .computeIfAbsent(partitionId, x -> new HashSet<>())
              .addAll(
                  updatedReassignServers.stream().map(x -> x.getId()).collect(Collectors.toSet()));

          if (serverHasReplaced) {
            for (ShuffleServerInfo serverInfo : updatedReassignServers) {
              newServerToPartitions
                  .computeIfAbsent(serverInfo, x -> new ArrayList<>())
                  .add(new PartitionRange(partitionId, partitionId));
            }
          }
        }
      }
      if (!newServerToPartitions.isEmpty()) {
        LOG.info(
            "Register the new partition->servers assignment on reassign. {}",
            newServerToPartitions);
        registerShuffleServers(id.get(), shuffleId, newServerToPartitions, getRemoteStorageInfo());
      }

      LOG.info(
          "Finished reassignOnBlockSendFailure request and cost {}(ms). Reassign result: {}",
          System.currentTimeMillis() - startTime,
          reassignResult);

      return handleInfo;
    }
  }

  /**
   * Creating the shuffleAssignmentInfo from the servers and partitionIds
   *
   * @param servers
   * @param partitionIds
   * @return
   */
  private ShuffleAssignmentsInfo createShuffleAssignmentsInfo(
      Set<ShuffleServerInfo> servers, Set<Integer> partitionIds) {
    Map<Integer, List<ShuffleServerInfo>> newPartitionToServers = new HashMap<>();
    List<PartitionRange> partitionRanges = new ArrayList<>();
    for (Integer partitionId : partitionIds) {
      newPartitionToServers.put(partitionId, new ArrayList<>(servers));
      partitionRanges.add(new PartitionRange(partitionId, partitionId));
    }
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
    for (ShuffleServerInfo server : servers) {
      serverToPartitionRanges.put(server, partitionRanges);
    }
    return new ShuffleAssignmentsInfo(newPartitionToServers, serverToPartitionRanges);
  }

  /** Request the new shuffle-servers to replace faulty server. */
  private Set<ShuffleServerInfo> reassignServerForTask(
      int shuffleId,
      Set<Integer> partitionIds,
      Set<String> excludedServers,
      int requiredServerNum) {
    AtomicReference<Set<ShuffleServerInfo>> replacementsRef =
        new AtomicReference<>(new HashSet<>());
    requestShuffleAssignment(
        shuffleId,
        requiredServerNum,
        1,
        requiredServerNum,
        1,
        excludedServers,
        shuffleAssignmentsInfo -> {
          if (shuffleAssignmentsInfo == null) {
            return null;
          }
          Set<ShuffleServerInfo> replacements =
              shuffleAssignmentsInfo.getPartitionToServers().values().stream()
                  .flatMap(x -> x.stream())
                  .collect(Collectors.toSet());
          replacementsRef.set(replacements);
          return createShuffleAssignmentsInfo(replacements, partitionIds);
        });
    return replacementsRef.get();
  }

  private Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      Function<ShuffleAssignmentsInfo, ShuffleAssignmentsInfo> reassignmentHandler) {
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    faultyServerIds.addAll(failuresShuffleServerIds);
    try {
      return RetryUtils.retry(
          () -> {
            ShuffleAssignmentsInfo response =
                shuffleWriteClient.getShuffleAssignments(
                    id.get(),
                    shuffleId,
                    partitionNum,
                    partitionNumPerRange,
                    assignmentTags,
                    assignmentShuffleServerNumber,
                    estimateTaskConcurrency,
                    faultyServerIds);
            LOG.info("Finished reassign");
            if (reassignmentHandler != null) {
              response = reassignmentHandler.apply(response);
            }
            registerShuffleServers(
                id.get(), shuffleId, response.getServerToPartitionRanges(), getRemoteStorageInfo());
            return response.getPartitionToServers();
          },
          retryInterval,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("registerShuffle failed!", throwable);
    }
  }

  private RemoteStorageInfo getRemoteStorageInfo() {
    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage =
        new RemoteStorageInfo(sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""));
    return ClientUtils.fetchRemoteStorage(
        id.get(), defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);
  }

  public boolean isRssResubmitStage() {
    return rssResubmitStage;
  }

  @VisibleForTesting
  public void setDataPusher(DataPusher dataPusher) {
    this.dataPusher = dataPusher;
  }

  @VisibleForTesting
  public Map<String, Set<Long>> getTaskToSuccessBlockIds() {
    return taskToSuccessBlockIds;
  }

  @VisibleForTesting
  public Map<String, FailedBlockSendTracker> getTaskToFailedBlockSendTracker() {
    return taskToFailedBlockSendTracker;
  }
}
