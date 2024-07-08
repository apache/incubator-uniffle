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

package org.apache.uniffle.shuffle.manager;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkException;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.RssStageInfo;
import org.apache.spark.shuffle.RssStageResubmitManager;
import org.apache.spark.shuffle.ShuffleHandleInfoManager;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.shuffle.SparkVersionUtils;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReassignOnStageRetryResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ExpireCloseableSupplier;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.shuffle.BlockIdManager;

import static org.apache.uniffle.common.config.RssClientConf.HADOOP_CONFIG_KEY_PREFIX;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED;

public abstract class RssShuffleManagerBase implements RssShuffleManagerInterface, ShuffleManager {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManagerBase.class);
  private AtomicBoolean isInitialized = new AtomicBoolean(false);
  private Method unregisterAllMapOutputMethod;
  private Method registerShuffleMethod;
  private volatile BlockIdManager blockIdManager;
  protected ShuffleDataDistributionType dataDistributionType;
  private Object blockIdManagerLock = new Object();
  protected AtomicReference<String> id = new AtomicReference<>();
  protected String appId = "";
  protected ShuffleWriteClient shuffleWriteClient;
  protected boolean dynamicConfEnabled;
  protected int maxConcurrencyPerPartitionToWrite;
  protected String clientType;

  protected SparkConf sparkConf;
  protected ExpireCloseableSupplier<ShuffleManagerClient> managerClientSupplier;
  protected boolean rssStageRetryEnabled;
  protected boolean rssStageRetryForWriteFailureEnabled;
  protected boolean rssStageRetryForFetchFailureEnabled;
  /**
   * Mapping between ShuffleId and ShuffleServer list. ShuffleServer list is dynamically allocated.
   * ShuffleServer is not obtained from RssShuffleHandle, but from this mapping.
   */
  protected ShuffleHandleInfoManager shuffleHandleInfoManager;

  protected RssStageResubmitManager rssStageResubmitManager;

  protected int partitionReassignMaxServerNum;

  protected boolean blockIdSelfManagedEnabled;

  protected boolean partitionReassignEnabled;

  protected boolean shuffleManagerRpcServiceEnabled;

  public BlockIdManager getBlockIdManager() {
    if (blockIdManager == null) {
      synchronized (blockIdManagerLock) {
        if (blockIdManager == null) {
          blockIdManager = new BlockIdManager();
          LOG.info("BlockId manager has been initialized.");
        }
      }
    }
    return blockIdManager;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    if (blockIdManager != null) {
      blockIdManager.remove(shuffleId);
    }
    return true;
  }

  /** See static overload of this method. */
  public abstract void configureBlockIdLayout(SparkConf sparkConf, RssConf rssConf);

  /**
   * Derives block id layout config from maximum number of allowed partitions. This value can be set
   * in either SparkConf or RssConf via RssSparkConfig.RSS_MAX_PARTITIONS, where SparkConf has
   * precedence.
   *
   * <p>Computes the number of required bits for partition id and task attempt id and reserves
   * remaining bits for sequence number. Adds RssClientConf.BLOCKID_SEQUENCE_NO_BITS,
   * RssClientConf.BLOCKID_PARTITION_ID_BITS, and RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS to the
   * given RssConf and adds them prefixed with "spark." to the given SparkConf.
   *
   * <p>If RssSparkConfig.RSS_MAX_PARTITIONS is not set, given values for
   * RssClientConf.BLOCKID_SEQUENCE_NO_BITS, RssClientConf.BLOCKID_PARTITION_ID_BITS, and
   * RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS are copied
   *
   * <p>Then, BlockIdLayout can consistently be created from both configs:
   *
   * <p>BlockIdLayout.from(rssConf) BlockIdLayout.from(RssSparkConfig.toRssConf(sparkConf))
   *
   * @param sparkConf Spark config providing max partitions
   * @param rssConf Rss config to amend
   * @param maxFailures Spark max failures
   * @param speculation Spark speculative execution
   */
  @VisibleForTesting
  protected static void configureBlockIdLayout(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    if (sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key())) {
      configureBlockIdLayoutFromMaxPartitions(sparkConf, rssConf, maxFailures, speculation);
    } else {
      configureBlockIdLayoutFromLayoutConfig(sparkConf, rssConf, maxFailures, speculation);
    }
  }

  private static void configureBlockIdLayoutFromMaxPartitions(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    int maxPartitions =
        sparkConf.getInt(
            RssSparkConfig.RSS_MAX_PARTITIONS.key(),
            RssSparkConfig.RSS_MAX_PARTITIONS.defaultValue().get());
    if (maxPartitions <= 1) {
      throw new IllegalArgumentException(
          "Value of "
              + RssSparkConfig.RSS_MAX_PARTITIONS.key()
              + " must be larger than 1: "
              + maxPartitions);
    }

    int attemptIdBits = getAttemptIdBits(getMaxAttemptNo(maxFailures, speculation));
    int partitionIdBits = 32 - Integer.numberOfLeadingZeros(maxPartitions - 1); // [1..31]
    int taskAttemptIdBits = partitionIdBits + attemptIdBits; // [1+attemptIdBits..31+attemptIdBits]
    int sequenceNoBits = 63 - partitionIdBits - taskAttemptIdBits; // [1-attemptIdBits..61]

    if (taskAttemptIdBits > 31) {
      throw new IllegalArgumentException(
          "Cannot support "
              + RssSparkConfig.RSS_MAX_PARTITIONS.key()
              + "="
              + maxPartitions
              + " partitions, "
              + "as this would require to reserve more than 31 bits "
              + "in the block id for task attempt ids. "
              + "With spark.maxFailures="
              + maxFailures
              + " and spark.speculation="
              + (speculation ? "true" : "false")
              + " at most "
              + (1 << (31 - attemptIdBits))
              + " partitions can be supported.");
    }

    // we have to cap the sequence number bits at 31 bits,
    // because BlockIdLayout imposes an upper bound of 31 bits
    // which is fine as this allows for over 2bn sequence ids
    if (sequenceNoBits > 31) {
      // move spare bits (bits over 31) from sequence number to partition id and task attempt id
      int spareBits = sequenceNoBits - 31;

      // make spareBits even, so we add same number of bits to partitionIdBits and taskAttemptIdBits
      spareBits += spareBits % 2;

      // move spare bits over
      partitionIdBits += spareBits / 2;
      taskAttemptIdBits += spareBits / 2;
      maxPartitions = (1 << partitionIdBits);

      // log with original sequenceNoBits
      if (LOG.isInfoEnabled()) {
        LOG.info(
            "Increasing "
                + RssSparkConfig.RSS_MAX_PARTITIONS.key()
                + " to "
                + maxPartitions
                + ", "
                + "otherwise we would have to support 2^"
                + sequenceNoBits
                + " (more than 2^31) sequence numbers.");
      }

      // remove spare bits
      sequenceNoBits -= spareBits;

      // propagate the change value back to SparkConf
      sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), String.valueOf(maxPartitions));
    }

    // set block id layout config in RssConf
    rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, sequenceNoBits);
    rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, partitionIdBits);
    rssConf.set(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, taskAttemptIdBits);

    // materialize these RssConf settings in sparkConf as well
    // so that RssSparkConfig.toRssConf(sparkConf) provides this configuration
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(sequenceNoBits));
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(partitionIdBits));
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(taskAttemptIdBits));
  }

  private static void configureBlockIdLayoutFromLayoutConfig(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    String sparkPrefix = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX;
    String sparkSeqNoBitsKey = sparkPrefix + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key();
    String sparkPartIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_PARTITION_ID_BITS.key();
    String sparkTaskIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key();

    // if one bit field is configured, all three must be given
    List<String> sparkKeys =
        Arrays.asList(sparkSeqNoBitsKey, sparkPartIdBitsKey, sparkTaskIdBitsKey);
    if (sparkKeys.stream().anyMatch(sparkConf::contains)
        && !sparkKeys.stream().allMatch(sparkConf::contains)) {
      String allKeys = sparkKeys.stream().collect(Collectors.joining(", "));
      String existingKeys =
          Arrays.stream(sparkConf.getAll())
              .map(t -> t._1)
              .filter(sparkKeys.stream().collect(Collectors.toSet())::contains)
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "All block id bit config keys must be provided ("
              + allKeys
              + "), not just a sub-set: "
              + existingKeys);
    }

    // if one bit field is configured, all three must be given
    List<ConfigOption<Integer>> rssKeys =
        Arrays.asList(
            RssClientConf.BLOCKID_SEQUENCE_NO_BITS,
            RssClientConf.BLOCKID_PARTITION_ID_BITS,
            RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS);
    if (rssKeys.stream().anyMatch(rssConf::contains)
        && !rssKeys.stream().allMatch(rssConf::contains)) {
      String allKeys = rssKeys.stream().map(ConfigOption::key).collect(Collectors.joining(", "));
      String existingKeys =
          rssConf.getKeySet().stream()
              .filter(rssKeys.stream().map(ConfigOption::key).collect(Collectors.toSet())::contains)
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "All block id bit config keys must be provided ("
              + allKeys
              + "), not just a sub-set: "
              + existingKeys);
    }

    if (sparkKeys.stream().allMatch(sparkConf::contains)) {
      rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, sparkConf.getInt(sparkSeqNoBitsKey, 0));
      rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, sparkConf.getInt(sparkPartIdBitsKey, 0));
      rssConf.set(
          RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, sparkConf.getInt(sparkTaskIdBitsKey, 0));
    } else if (rssKeys.stream().allMatch(rssConf::contains)) {
      sparkConf.set(sparkSeqNoBitsKey, rssConf.getValue(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
      sparkConf.set(sparkPartIdBitsKey, rssConf.getValue(RssClientConf.BLOCKID_PARTITION_ID_BITS));
      sparkConf.set(
          sparkTaskIdBitsKey, rssConf.getValue(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    } else {
      // use default max partitions
      sparkConf.set(
          RssSparkConfig.RSS_MAX_PARTITIONS.key(),
          RssSparkConfig.RSS_MAX_PARTITIONS.defaultValueString());
      configureBlockIdLayoutFromMaxPartitions(sparkConf, rssConf, maxFailures, speculation);
    }
  }

  protected static int getMaxAttemptNo(int maxFailures, boolean speculation) {
    // attempt number is zero based: 0, 1, …, maxFailures-1
    // max maxFailures < 1 is not allowed but for safety, we interpret that as maxFailures == 1
    int maxAttemptNo = maxFailures < 1 ? 0 : maxFailures - 1;

    // with speculative execution enabled we could observe +1 attempts
    if (speculation) {
      maxAttemptNo++;
    }

    return maxAttemptNo;
  }

  protected static int getAttemptIdBits(int maxAttemptNo) {
    return 32 - Integer.numberOfLeadingZeros(maxAttemptNo);
  }

  /** See static overload of this method. */
  public abstract long getTaskAttemptIdForBlockId(int mapIndex, int attemptNo);

  /**
   * Provides a task attempt id to be used in the block id, that is unique for a shuffle stage.
   *
   * <p>We are not using context.taskAttemptId() here as this is a monotonically increasing number
   * that is unique across the entire Spark app which can reach very large numbers, which can
   * practically reach LONG.MAX_VALUE. That would overflow the bits in the block id.
   *
   * <p>Here we use the map index or task id, appended by the attempt number per task. The map index
   * is limited by the number of partitions of a stage. The attempt number per task is limited /
   * configured by spark.task.maxFailures (default: 4).
   *
   * @return a task attempt id unique for a shuffle stage
   */
  protected static long getTaskAttemptIdForBlockId(
      int mapIndex, int attemptNo, int maxFailures, boolean speculation, int maxTaskAttemptIdBits) {
    int maxAttemptNo = getMaxAttemptNo(maxFailures, speculation);
    int attemptBits = getAttemptIdBits(maxAttemptNo);

    if (attemptNo > maxAttemptNo) {
      // this should never happen, if it does, our assumptions are wrong,
      // and we risk overflowing the attempt number bits
      throw new RssException(
          "Observing attempt number "
              + attemptNo
              + " while maxFailures is set to "
              + maxFailures
              + (speculation ? " with speculation enabled" : "")
              + ".");
    }

    int mapIndexBits = 32 - Integer.numberOfLeadingZeros(mapIndex);
    if (mapIndexBits + attemptBits > maxTaskAttemptIdBits) {
      throw new RssException(
          "Observing mapIndex["
              + mapIndex
              + "] that would produce a taskAttemptId with "
              + (mapIndexBits + attemptBits)
              + " bits which is larger than the allowed "
              + maxTaskAttemptIdBits
              + " bits (maxFailures["
              + maxFailures
              + "], speculation["
              + speculation
              + "]). Please consider providing more bits for taskAttemptIds.");
    }

    return (long) mapIndex << attemptBits | attemptNo;
  }

  protected static void fetchAndApplyDynamicConf(SparkConf sparkConf) {
    String clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM.key());
    CoordinatorClientFactory coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    List<CoordinatorClient> coordinatorClients =
        coordinatorClientFactory.createCoordinatorClient(
            ClientType.valueOf(clientType), coordinators);

    int timeoutMs =
        sparkConf.getInt(
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.key(),
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.defaultValue().get());
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      throw new RssException("Errors on getting current user.", e);
    }
    RssFetchClientConfRequest request =
        new RssFetchClientConfRequest(timeoutMs, user, Collections.emptyMap());
    for (CoordinatorClient client : coordinatorClients) {
      RssFetchClientConfResponse response = client.fetchClientConf(request);
      if (response.getStatusCode() == StatusCode.SUCCESS) {
        LOG.info("Success to get conf from {}", client.getDesc());
        RssSparkShuffleUtils.applyDynamicClientConf(sparkConf, response.getClientConf());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", client.getDesc());
      }
    }

    coordinatorClients.forEach(CoordinatorClient::close);
  }

  @Override
  public void unregisterAllMapOutput(int shuffleId) throws SparkException {
    if (!RssSparkShuffleUtils.isStageResubmitSupported()) {
      return;
    }
    MapOutputTrackerMaster tracker = getMapOutputTrackerMaster();
    if (isInitialized.compareAndSet(false, true)) {
      unregisterAllMapOutputMethod = getUnregisterAllMapOutputMethod(tracker);
      registerShuffleMethod = getRegisterShuffleMethod(tracker);
    }
    if (unregisterAllMapOutputMethod != null) {
      try {
        unregisterAllMapOutputMethod.invoke(tracker, shuffleId);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RssException("Invoke unregisterAllMapOutput method failed", e);
      }
    } else {
      int numMaps = getNumMaps(shuffleId);
      int numReduces = getPartitionNum(shuffleId);
      defaultUnregisterAllMapOutput(tracker, registerShuffleMethod, shuffleId, numMaps, numReduces);
    }
  }

  private static void defaultUnregisterAllMapOutput(
      MapOutputTrackerMaster tracker,
      Method registerShuffle,
      int shuffleId,
      int numMaps,
      int numReduces)
      throws SparkException {
    if (tracker != null && registerShuffle != null) {
      tracker.unregisterShuffle(shuffleId);
      // re-register this shuffle id into map output tracker
      try {
        if (SparkVersionUtils.MAJOR_VERSION > 3
            || (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION >= 2)) {
          registerShuffle.invoke(tracker, shuffleId, numMaps, numReduces);
        } else {
          registerShuffle.invoke(tracker, shuffleId, numMaps);
        }
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RssException("Invoke registerShuffle method failed", e);
      }
      tracker.incrementEpoch();
    } else {
      throw new SparkException(
          "default unregisterAllMapOutput should only be called on the driver side");
    }
  }

  private static Method getUnregisterAllMapOutputMethod(MapOutputTrackerMaster tracker) {
    if (tracker != null) {
      Class<? extends MapOutputTrackerMaster> klass = tracker.getClass();
      Method m = null;
      try {
        if (SparkVersionUtils.isSpark2() && SparkVersionUtils.MINOR_VERSION <= 3) {
          // for spark version less than 2.3, there's no unregisterAllMapOutput support
          LOG.warn("Spark version <= 2.3, fallback to default method");
        } else if (SparkVersionUtils.isSpark2()) {
          // this method is added in Spark 2.4+
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION <= 1) {
          // spark 3.1 will have unregisterAllMapOutput method
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3()) {
          m = klass.getDeclaredMethod("unregisterAllMapAndMergeOutput", int.class);
        } else {
          LOG.warn(
              "Unknown spark version({}), fallback to default method",
              SparkVersionUtils.SPARK_VERSION);
        }
      } catch (NoSuchMethodException e) {
        LOG.warn(
            "Got no such method error when get unregisterAllMapOutput method for spark version({})",
            SparkVersionUtils.SPARK_VERSION);
      }
      return m;
    } else {
      return null;
    }
  }

  private static Method getRegisterShuffleMethod(MapOutputTrackerMaster tracker) {
    if (tracker != null) {
      Class<? extends MapOutputTrackerMaster> klass = tracker.getClass();
      Method m = null;
      try {
        if (SparkVersionUtils.MAJOR_VERSION > 3
            || (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION >= 2)) {
          // for spark >= 3.2, the register shuffle method is changed to signature:
          //   registerShuffle(shuffleId, numMapTasks, numReduceTasks);
          m = klass.getDeclaredMethod("registerShuffle", int.class, int.class, int.class);
        } else {
          m = klass.getDeclaredMethod("registerShuffle", int.class, int.class);
        }
      } catch (NoSuchMethodException e) {
        LOG.warn(
            "Got no such method error when get registerShuffle method for spark version({})",
            SparkVersionUtils.SPARK_VERSION);
      }
      return m;
    } else {
      return null;
    }
  }

  private static MapOutputTrackerMaster getMapOutputTrackerMaster() {
    MapOutputTracker tracker =
        Optional.ofNullable(SparkEnv.get()).map(SparkEnv::mapOutputTracker).orElse(null);
    return tracker instanceof MapOutputTrackerMaster ? (MapOutputTrackerMaster) tracker : null;
  }

  private static Map<String, String> parseRemoteStorageConf(Configuration conf) {
    Map<String, String> confItems = Maps.newHashMap();
    for (Map.Entry<String, String> entry : conf) {
      confItems.put(entry.getKey(), entry.getValue());
    }
    return confItems;
  }

  protected static RemoteStorageInfo getDefaultRemoteStorageInfo(SparkConf sparkConf) {
    Map<String, String> confItems = Maps.newHashMap();
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    if (rssConf.getBoolean(RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED)) {
      confItems = parseRemoteStorageConf(new Configuration(true));
    }

    for (String key : rssConf.getKeySet()) {
      if (key.startsWith(HADOOP_CONFIG_KEY_PREFIX)) {
        String val = rssConf.getString(key, null);
        if (val != null) {
          String extractedKey = key.replaceFirst(HADOOP_CONFIG_KEY_PREFIX, "");
          confItems.put(extractedKey, val);
        }
      }
    }

    return new RemoteStorageInfo(
        sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""), confItems);
  }

  /**
   * In Stage Retry mode, obtain the Shuffle Server list from the Driver based on shuffleId.
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  protected synchronized StageAttemptShuffleHandleInfo getRemoteShuffleHandleInfoWithStageRetry(
      int shuffleId) {
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(shuffleId);
    RssReassignOnStageRetryResponse rpcPartitionToShufflerServer =
        getOrCreateShuffleManagerClientWrapper()
            .get()
            .getPartitionToShufflerServerWithStageRetry(rssPartitionToShuffleServerRequest);
    StageAttemptShuffleHandleInfo shuffleHandleInfo =
        StageAttemptShuffleHandleInfo.fromProto(
            rpcPartitionToShufflerServer.getShuffleHandleInfoProto());
    return shuffleHandleInfo;
  }

  /**
   * In Block Retry mode, obtain the Shuffle Server list from the Driver based on shuffleId.
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  protected synchronized MutableShuffleHandleInfo getRemoteShuffleHandleInfoWithBlockRetry(
      int shuffleId) {
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(shuffleId);
    RssReassignOnBlockSendFailureResponse rpcPartitionToShufflerServer =
        getOrCreateShuffleManagerClientWrapper()
            .get()
            .getPartitionToShufflerServerWithBlockRetry(rssPartitionToShuffleServerRequest);
    MutableShuffleHandleInfo shuffleHandleInfo =
        MutableShuffleHandleInfo.fromProto(rpcPartitionToShufflerServer.getHandle());
    return shuffleHandleInfo;
  }

  protected synchronized ExpireCloseableSupplier<ShuffleManagerClient>
      getOrCreateShuffleManagerClientWrapper() {
    if (managerClientSupplier == null) {
      RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
      String driver = rssConf.getString("driver.host", "");
      int port = rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT);
      long rpcTimeout = rssConf.getLong(RssBaseConf.RSS_CLIENT_TYPE_GRPC_TIMEOUT_MS);
      this.managerClientSupplier =
          new ExpireCloseableSupplier<>(
              () ->
                  ShuffleManagerClientFactory.getInstance()
                      .createShuffleManagerClient(ClientType.GRPC, driver, port, rpcTimeout));
    }
    return managerClientSupplier;
  }

  @Override
  public ShuffleHandleInfo getShuffleHandleInfoByShuffleId(int shuffleId) {
    return shuffleHandleInfoManager.get(shuffleId);
  }

  /**
   * @return the maximum number of fetch failures per shuffle partition before that shuffle stage
   *     should be recomputed
   */
  @Override
  public int getMaxFetchFailures() {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    return Math.max(0, sparkConf.getInt(TASK_MAX_FAILURE, 4) - 1);
  }

  /**
   * Add the shuffleServer that failed to write to the failure list
   *
   * @param shuffleServerId
   */
  @Override
  public void addFailuresShuffleServerInfos(String shuffleServerId) {
    rssStageResubmitManager.recordFailuresShuffleServer(shuffleServerId);
  }

  /**
   * Reassign the ShuffleServer list for ShuffleId
   *
   * @param shuffleId
   * @param numPartitions
   */
  @Override
  public boolean reassignOnStageResubmit(
      int stageId, int stageAttemptNumber, int shuffleId, int numPartitions) {
    String stageIdAndAttempt = stageId + "_" + stageAttemptNumber;
    RssStageInfo rssStageInfo =
        rssStageResubmitManager.recordAndGetServerAssignedInfo(shuffleId, stageIdAndAttempt);
    synchronized (rssStageInfo) {
      Boolean needReassign = rssStageInfo.isReassigned();
      if (!needReassign) {
        int requiredShuffleServerNumber =
            RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
        int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);

        /**
         * this will clear up the previous stage attempt all data when registering the same
         * shuffleId at the second time
         */
        Map<Integer, List<ShuffleServerInfo>> partitionToServers =
            requestShuffleAssignment(
                shuffleId,
                numPartitions,
                1,
                requiredShuffleServerNumber,
                estimateTaskConcurrency,
                rssStageResubmitManager.getServerIdBlackList(),
                stageId,
                stageAttemptNumber,
                false);
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
        MutableShuffleHandleInfo shuffleHandleInfo =
            new MutableShuffleHandleInfo(shuffleId, partitionToServers, getRemoteStorageInfo());
        StageAttemptShuffleHandleInfo stageAttemptShuffleHandleInfo =
            (StageAttemptShuffleHandleInfo) shuffleHandleInfoManager.get(shuffleId);
        stageAttemptShuffleHandleInfo.replaceCurrentShuffleHandleInfo(shuffleHandleInfo);
        rssStageResubmitManager.recordAndGetServerAssignedInfo(shuffleId, stageIdAndAttempt, true);
        LOG.info(
            "The stage retry has been triggered successfully for the stageId: {}, attemptNumber: {}",
            stageId,
            stageAttemptNumber);
        return true;
      } else {
        LOG.info(
            "Do nothing that the stage: {} has been reassigned for attempt{}",
            stageId,
            stageAttemptNumber);
        return false;
      }
    }
  }

  /** this is only valid on driver side that exposed to being invoked by grpc server */
  @Override
  public MutableShuffleHandleInfo reassignOnBlockSendFailure(
      int stageId,
      int stageAttemptNumber,
      int shuffleId,
      Map<Integer, List<ReceivingFailureServer>> partitionToFailureServers) {
    long startTime = System.currentTimeMillis();
    ShuffleHandleInfo handleInfo = shuffleHandleInfoManager.get(shuffleId);
    MutableShuffleHandleInfo internalHandle = null;
    if (handleInfo instanceof MutableShuffleHandleInfo) {
      internalHandle = (MutableShuffleHandleInfo) handleInfo;
    } else if (handleInfo instanceof StageAttemptShuffleHandleInfo) {
      internalHandle =
          (MutableShuffleHandleInfo) ((StageAttemptShuffleHandleInfo) handleInfo).getCurrent();
    }
    if (internalHandle == null) {
      throw new RssException(
          "An unexpected error occurred: internalHandle is null, which should not happen");
    }
    synchronized (internalHandle) {
      // If the reassignment servers for one partition exceeds the max reassign server num,
      // it should fast fail.
      internalHandle.checkPartitionReassignServerNum(
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
          Set<ShuffleServerInfo> replacements = internalHandle.getReplacements(serverId);
          if (CollectionUtils.isEmpty(replacements)) {
            final int requiredServerNum = 1;
            Set<String> excludedServers = new HashSet<>(internalHandle.listExcludedServers());
            excludedServers.add(serverId);
            replacements =
                reassignServerForTask(
                    stageId,
                    stageAttemptNumber,
                    shuffleId,
                    Sets.newHashSet(partitionId),
                    excludedServers,
                    requiredServerNum,
                    true);
          } else {
            serverHasReplaced = true;
          }

          Set<ShuffleServerInfo> updatedReassignServers =
              internalHandle.updateAssignment(partitionId, serverId, replacements);

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

      return internalHandle;
    }
  }

  @Override
  public void stop() {
    if (managerClientSupplier != null) {
      managerClientSupplier.forceClose();
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
      int stageId,
      int stageAttemptNumber,
      int shuffleId,
      Set<Integer> partitionIds,
      Set<String> excludedServers,
      int requiredServerNum,
      boolean reassign) {
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
        },
        stageId,
        stageAttemptNumber,
        reassign);
    return replacementsRef.get();
  }

  private Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      Function<ShuffleAssignmentsInfo, ShuffleAssignmentsInfo> reassignmentHandler,
      int stageId,
      int stageAttemptNumber,
      boolean reassign) {
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
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
                    faultyServerIds,
                    stageId,
                    stageAttemptNumber,
                    reassign);
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

  protected Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageId,
      int stageAttemptNumber,
      boolean reassign) {
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
    try {
      return RetryUtils.retry(
          () -> {
            ShuffleAssignmentsInfo response =
                shuffleWriteClient.getShuffleAssignments(
                    appId,
                    shuffleId,
                    partitionNum,
                    partitionNumPerRange,
                    assignmentTags,
                    assignmentShuffleServerNumber,
                    estimateTaskConcurrency,
                    faultyServerIds,
                    stageId,
                    stageAttemptNumber,
                    reassign);
            registerShuffleServers(
                appId,
                shuffleId,
                response.getServerToPartitionRanges(),
                getRemoteStorageInfo(),
                stageAttemptNumber);
            return response.getPartitionToServers();
          },
          retryInterval,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("registerShuffle failed!", throwable);
    }
  }

  protected Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageAttemptNumber) {
    return requestShuffleAssignment(
        shuffleId,
        partitionNum,
        partitionNumPerRange,
        assignmentShuffleServerNumber,
        estimateTaskConcurrency,
        faultyServerIds,
        -1,
        stageAttemptNumber,
        false);
  }

  protected void registerShuffleServers(
      String appId,
      int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      RemoteStorageInfo remoteStorage,
      int stageAttemptNumber) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId {}", shuffleId);
    long start = System.currentTimeMillis();
    serverToPartitionRanges.entrySet().stream()
        .forEach(
            entry -> {
              shuffleWriteClient.registerShuffle(
                  entry.getKey(),
                  appId,
                  shuffleId,
                  entry.getValue(),
                  remoteStorage,
                  ShuffleDataDistributionType.NORMAL,
                  maxConcurrencyPerPartitionToWrite,
                  stageAttemptNumber);
            });
    LOG.info(
        "Finish register shuffleId {} with {} ms", shuffleId, (System.currentTimeMillis() - start));
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
    LOG.info("Start to register shuffleId[{}]", shuffleId);
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
        "Finish register shuffleId[{}] with {} ms",
        shuffleId,
        (System.currentTimeMillis() - start));
  }

  protected RemoteStorageInfo getRemoteStorageInfo() {
    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage =
        new RemoteStorageInfo(sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""));
    return ClientUtils.fetchRemoteStorage(
        appId, defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);
  }

  public boolean isRssStageRetryEnabled() {
    return rssStageRetryEnabled;
  }

  public boolean isRssStageRetryForWriteFailureEnabled() {
    return rssStageRetryForWriteFailureEnabled;
  }

  public boolean isRssStageRetryForFetchFailureEnabled() {
    return rssStageRetryForFetchFailureEnabled;
  }
}
