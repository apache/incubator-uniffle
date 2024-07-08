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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.DataPusher;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.RssShuffleClientFactory;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.ShuffleManagerGrpcService;
import org.apache.uniffle.shuffle.manager.ShuffleManagerServerFactory;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED;
import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;
import static org.apache.uniffle.common.config.RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE;

public class RssShuffleManager extends RssShuffleManagerBase {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final long heartbeatInterval;
  private final long heartbeatTimeout;
  private ScheduledExecutorService heartBeatScheduledExecutorService;
  private Map<String, Set<Long>> taskToSuccessBlockIds = JavaUtils.newConcurrentMap();
  private Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker =
      JavaUtils.newConcurrentMap();
  private final int dataReplica;
  private final int dataReplicaWrite;
  private final int dataReplicaRead;
  private final boolean dataReplicaSkipEnabled;
  private final int dataTransferPoolSize;
  private final int dataCommitPoolSize;
  private Set<String> failedTaskIds = Sets.newConcurrentHashSet();
  private boolean heartbeatStarted = false;
  private final int maxFailures;
  private final boolean speculation;
  private final BlockIdLayout blockIdLayout;
  private final String user;
  private final String uuid;
  private DataPusher dataPusher;
  private final Map<Integer, Integer> shuffleIdToPartitionNum = JavaUtils.newConcurrentMap();
  private final Map<Integer, Integer> shuffleIdToNumMapTasks = JavaUtils.newConcurrentMap();
  private GrpcServer shuffleManagerServer;
  private ShuffleManagerGrpcService service;

  public RssShuffleManager(SparkConf sparkConf, boolean isDriver) {
    if (sparkConf.getBoolean("spark.sql.adaptive.enabled", false)) {
      throw new IllegalArgumentException(
          "Spark2 doesn't support AQE, spark.sql.adaptive.enabled should be false.");
    }
    this.sparkConf = sparkConf;
    this.user = sparkConf.get("spark.rss.quota.user", "user");
    this.uuid = sparkConf.get("spark.rss.quota.uuid", Long.toString(System.currentTimeMillis()));
    this.dynamicConfEnabled = sparkConf.get(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);

    // fetch client conf and apply them if necessary
    if (isDriver && this.dynamicConfEnabled) {
      fetchAndApplyDynamicConf(sparkConf);
    }
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);

    // configure block id layout
    this.maxFailures = sparkConf.getInt("spark.task.maxFailures", 4);
    this.speculation = sparkConf.getBoolean("spark.speculation", false);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    // configureBlockIdLayout requires maxFailures and speculation to be initialized
    configureBlockIdLayout(sparkConf, rssConf);
    this.blockIdLayout = BlockIdLayout.from(rssConf);

    // set & check replica config
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    this.maxConcurrencyPerPartitionToWrite =
        RssSparkConfig.toRssConf(sparkConf).get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    LOG.info(
        "Check quorum config [{}:{}:{}:{}]",
        dataReplica,
        dataReplicaWrite,
        dataReplicaRead,
        dataReplicaSkipEnabled);
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout =
        sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    int unregisterThreadPoolSize =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterTimeoutSec = sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_TIMEOUT_SEC);
    int unregisterRequestTimeoutSec =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    // External shuffle service is not supported when using remote shuffle service
    sparkConf.set("spark.shuffle.service.enabled", "false");
    LOG.info("Disable external shuffle service in RssShuffleManager.");
    // If we store shuffle data in distributed filesystem or in a disaggregated
    // shuffle cluster, we don't need shuffle data locality
    sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
    LOG.info("Disable shuffle data locality in RssShuffleManager.");

    // stage retry for write/fetch failure
    rssStageRetryForFetchFailureEnabled =
        rssConf.get(RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED);
    rssStageRetryForWriteFailureEnabled =
        rssConf.get(RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED);
    if (rssStageRetryForFetchFailureEnabled || rssStageRetryForWriteFailureEnabled) {
      rssStageRetryEnabled = true;
      List<String> logTips = new ArrayList<>();
      if (rssStageRetryForWriteFailureEnabled) {
        logTips.add("write");
      }
      if (rssStageRetryForWriteFailureEnabled) {
        logTips.add("fetch");
      }
      LOG.info(
          "Activate the stage retry mechanism that will resubmit stage on {} failure",
          StringUtils.join(logTips, "/"));
    }
    this.partitionReassignEnabled = rssConf.getBoolean(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED);
    this.blockIdSelfManagedEnabled = rssConf.getBoolean(RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED);
    this.shuffleManagerRpcServiceEnabled =
        partitionReassignEnabled || rssStageRetryEnabled || blockIdSelfManagedEnabled;
    if (!sparkConf.getBoolean(RssSparkConfig.RSS_TEST_FLAG.key(), false)) {
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
        getOrCreateShuffleManagerClientWrapper();
      }
      this.shuffleWriteClient =
          RssShuffleClientFactory.getInstance()
              .createShuffleWriteClient(
                  RssShuffleClientFactory.newWriteBuilder()
                      .blockIdSelfManagedEnabled(blockIdSelfManagedEnabled)
                      .managerClientSupplier(managerClientSupplier)
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
                      .unregisterTimeSec(unregisterTimeoutSec)
                      .unregisterRequestTimeSec(unregisterRequestTimeoutSec)
                      .rssConf(rssConf));
      registerCoordinator();

      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data pusher is starting...");
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
    }
    this.shuffleHandleInfoManager = new ShuffleHandleInfoManager();
    this.rssStageResubmitManager = new RssStageResubmitManager();
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader).
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {

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

    // If yarn enable retry ApplicationMaster, appId will be not unique and shuffle data will be
    // incorrect,
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
          shuffleId, appId, dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = getDefaultRemoteStorageInfo(sparkConf);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            appId, defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);

    // get all register info according to coordinator's response
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    int requiredShuffleServerNumber =
        RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        requestShuffleAssignment(
            shuffleId,
            dependency.partitioner().numPartitions(),
            1,
            requiredShuffleServerNumber,
            estimateTaskConcurrency,
            rssStageResubmitManager.getServerIdBlackList(),
            0);

    startHeartbeat();

    shuffleIdToPartitionNum.putIfAbsent(shuffleId, dependency.partitioner().numPartitions());
    shuffleIdToNumMapTasks.putIfAbsent(shuffleId, dependency.rdd().partitions().length);
    if (shuffleManagerRpcServiceEnabled && rssStageRetryEnabled) {
      ShuffleHandleInfo handleInfo =
          new MutableShuffleHandleInfo(shuffleId, partitionToServers, remoteStorage);
      StageAttemptShuffleHandleInfo stageAttemptShuffleHandleInfo =
          new StageAttemptShuffleHandleInfo(shuffleId, remoteStorage, handleInfo);
      shuffleHandleInfoManager.register(shuffleId, stageAttemptShuffleHandleInfo);
    } else if (shuffleManagerRpcServiceEnabled && partitionReassignEnabled) {
      ShuffleHandleInfo shuffleHandleInfo =
          new MutableShuffleHandleInfo(shuffleId, partitionToServers, remoteStorage);
      shuffleHandleInfoManager.register(shuffleId, shuffleHandleInfo);
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
            + "]");
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
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, int mapId, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle<K, V, ?> rssHandle = (RssShuffleHandle<K, V, ?>) handle;
      appId = rssHandle.getAppId();
      dataPusher.setRssAppId(appId);

      int shuffleId = rssHandle.getShuffleId();
      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      ShuffleHandleInfo shuffleHandleInfo;
      if (shuffleManagerRpcServiceEnabled && rssStageRetryEnabled) {
        // In Stage Retry mode, Get the ShuffleServer list from the Driver based on the shuffleId
        shuffleHandleInfo = getRemoteShuffleHandleInfoWithStageRetry(shuffleId);
      } else if (shuffleManagerRpcServiceEnabled && partitionReassignEnabled) {
        // In Block Retry mode, Get the ShuffleServer list from the Driver based on the shuffleId
        shuffleHandleInfo = getRemoteShuffleHandleInfoWithBlockRetry(shuffleId);
      } else {
        shuffleHandleInfo =
            new SimpleShuffleHandleInfo(
                shuffleId, rssHandle.getPartitionToServers(), rssHandle.getRemoteStorage());
      }
      ShuffleWriteMetrics writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      return new RssShuffleWriter<>(
          rssHandle.getAppId(),
          shuffleId,
          taskId,
          getTaskAttemptIdForBlockId(context.partitionId(), context.attemptNumber()),
          writeMetrics,
          this,
          sparkConf,
          shuffleWriteClient,
          managerClientSupplier,
          rssHandle,
          this::markFailedTask,
          context,
          shuffleHandleInfo);
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  /**
   * Derives block id layout config from maximum number of allowed partitions. Computes the number
   * of required bits for partition id and task attempt id and reserves remaining bits for sequence
   * number.
   *
   * @param sparkConf Spark config providing max partitions
   * @param rssConf Rss config to amend
   */
  public void configureBlockIdLayout(SparkConf sparkConf, RssConf rssConf) {
    configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
  }

  @Override
  public long getTaskAttemptIdForBlockId(int mapIndex, int attemptNo) {
    return getTaskAttemptIdForBlockId(
        mapIndex, attemptNo, maxFailures, speculation, blockIdLayout.taskAttemptIdBits);
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle<K, C, ?> rssShuffleHandle = (RssShuffleHandle<K, C, ?>) handle;
      final int partitionNumPerRange = sparkConf.get(RssSparkConfig.RSS_PARTITION_NUM_PER_RANGE);
      final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
      int shuffleId = rssShuffleHandle.getShuffleId();
      long start = System.currentTimeMillis();
      Roaring64NavigableMap taskIdBitmap =
          getExpectedTasks(shuffleId, startPartition, endPartition);
      LOG.info(
          "Get taskId cost "
              + (System.currentTimeMillis() - start)
              + " ms, and request expected blockIds from "
              + taskIdBitmap.getLongCardinality()
              + " tasks for shuffleId["
              + shuffleId
              + "], partitionId["
              + startPartition
              + "]");
      start = System.currentTimeMillis();
      ShuffleHandleInfo shuffleHandleInfo;
      if (shuffleManagerRpcServiceEnabled && rssStageRetryEnabled) {
        // In Stage Retry mode, Get the ShuffleServer list from the Driver based on the shuffleId.
        shuffleHandleInfo = getRemoteShuffleHandleInfoWithStageRetry(shuffleId);
      } else if (shuffleManagerRpcServiceEnabled && partitionReassignEnabled) {
        // In Block Retry mode, Get the ShuffleServer list from the Driver based on the shuffleId
        shuffleHandleInfo = getRemoteShuffleHandleInfoWithBlockRetry(shuffleId);
      } else {
        shuffleHandleInfo =
            new SimpleShuffleHandleInfo(
                shuffleId,
                rssShuffleHandle.getPartitionToServers(),
                rssShuffleHandle.getRemoteStorage());
      }
      Map<Integer, List<ShuffleServerInfo>> partitionToServers =
          shuffleHandleInfo.getAllPartitionServersForReader();
      Roaring64NavigableMap blockIdBitmap =
          getShuffleResult(
              clientType,
              Sets.newHashSet(partitionToServers.get(startPartition)),
              rssShuffleHandle.getAppId(),
              shuffleId,
              startPartition,
              context.stageAttemptNumber());
      LOG.info(
          "Get shuffle blockId cost "
              + (System.currentTimeMillis() - start)
              + " ms, and get "
              + blockIdBitmap.getLongCardinality()
              + " blockIds for shuffleId["
              + shuffleId
              + "], partitionId["
              + startPartition
              + "]");

      final RemoteStorageInfo shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage();
      LOG.info("Shuffle reader using remote storage {}", shuffleRemoteStorageInfo);
      final String shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath();
      Configuration readerHadoopConf =
          RssSparkShuffleUtils.getRemoteStorageHadoopConf(sparkConf, shuffleRemoteStorageInfo);

      return new RssShuffleReader<K, C>(
          startPartition,
          endPartition,
          context,
          rssShuffleHandle,
          shuffleRemoteStoragePath,
          readerHadoopConf,
          partitionNumPerRange,
          partitionNum,
          blockIdBitmap,
          taskIdBitmap,
          RssSparkConfig.toRssConf(sparkConf),
          partitionToServers,
          managerClientSupplier);
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      int startMapId,
      int endMapId) {
    return null;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    try {
      super.unregisterShuffle(shuffleId);
      if (SparkEnv.get().executorId().equals("driver")) {
        shuffleWriteClient.unregisterShuffle(appId, shuffleId);
        shuffleIdToNumMapTasks.remove(shuffleId);
        shuffleIdToPartitionNum.remove(shuffleId);
        if (service != null) {
          service.unregisterShuffle(shuffleId);
        }
      }
    } catch (Exception e) {
      LOG.warn("Errors on unregistering from remote shuffle-servers", e);
    }
    return true;
  }

  @Override
  public void stop() {
    super.stop();
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
    if (shuffleWriteClient != null) {
      // Unregister shuffle before closing shuffle write client.
      shuffleWriteClient.unregisterShuffle(appId);
      shuffleWriteClient.close();
    }
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RssException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  // when speculation enable, duplicate data will be sent and reported to shuffle server,
  // get the actual tasks and filter the duplicate data caused by speculation task
  private Roaring64NavigableMap getExpectedTasks(
      int shuffleId, int startPartition, int endPartition) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    // In 2.3, getMapSizesByExecutorId returns Seq, while it returns Iterator in 2.4,
    // so we use toIterator() to support Spark 2.3 & 2.4
    Iterator<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapStatusIter =
        SparkEnv.get()
            .mapOutputTracker()
            .getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
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

  @VisibleForTesting
  public void addSuccessBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToSuccessBlockIds.get(taskId) == null) {
      taskToSuccessBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToSuccessBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void addFailedBlockSendTracker(
      String taskId, FailedBlockSendTracker failedBlockSendTracker) {
    taskToFailedBlockSendTracker.putIfAbsent(taskId, failedBlockSendTracker);
  }

  public void clearTaskMeta(String taskId) {
    taskToSuccessBlockIds.remove(taskId);
    taskToFailedBlockSendTracker.remove(taskId);
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

  /** @return the unique spark id for rss shuffle */
  @Override
  public String getAppId() {
    return appId;
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

  private Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId,
      int stageAttemptId) {
    try {
      return shuffleWriteClient.getShuffleResult(
          clientType, shuffleServerInfoSet, appId, shuffleId, partitionId);
    } catch (RssFetchFailedException e) {
      throw RssSparkShuffleUtils.reportRssFetchFailedException(
          managerClientSupplier,
          e,
          sparkConf,
          appId,
          shuffleId,
          stageAttemptId,
          Sets.newHashSet(partitionId));
    }
  }

  public FailedBlockSendTracker getBlockIdsFailedSendTracker(String taskId) {
    return taskToFailedBlockSendTracker.get(taskId);
  }

  private ShuffleServerInfo assignShuffleServer(int shuffleId, String faultyShuffleServerId) {
    Set<String> faultyServerIds = Sets.newHashSet(faultyShuffleServerId);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        requestShuffleAssignment(shuffleId, 1, 1, 1, 1, faultyServerIds, 0);
    if (partitionToServers.get(0) != null && partitionToServers.get(0).size() == 1) {
      return partitionToServers.get(0).get(0);
    }
    return null;
  }
}
