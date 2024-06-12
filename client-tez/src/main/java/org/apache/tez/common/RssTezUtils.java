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

package org.apache.tez.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.OrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.RssConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.RssConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.input.RssOrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.input.RssOrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.RssOrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.RssUnorderedKVInput;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;

public class RssTezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssTezUtils.class);
  private static final BlockIdLayout LAYOUT = BlockIdLayout.DEFAULT;
  private static final int MAX_ATTEMPT_LENGTH = 6;
  private static final int MAX_ATTEMPT_ID = (1 << MAX_ATTEMPT_LENGTH) - 1;
  private static final int MAX_SEQUENCE_NO =
      (1 << (LAYOUT.sequenceNoBits - MAX_ATTEMPT_LENGTH)) - 1;

  public static final String UNDERLINE_DELIMITER = "_";
  // constant to compute shuffle id
  private static final int VERTEX_ID_MAPPING_MAX_ID = 500;
  private static final String VERTEX_ID_MAPPING_MAP = "Map";
  private static final String VERTEX_ID_MAPPING_REDUCER = "Reducer";
  private static final int VERTEX_ID_MAPPING_MAGIC = 600;
  private static final int SHUFFLE_ID_MAGIC = 1000;

  private RssTezUtils() {}

  public static ShuffleWriteClient createShuffleClient(TezClientConf conf) {
    int heartBeatThreadNum = conf.get(TezClientConf.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    int retryMax = conf.get(TezClientConf.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = conf.get(TezClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX);

    String clientType = conf.get(TezClientConf.RSS_CLIENT_TYPE);
    int replicaWrite = conf.get(TezClientConf.RSS_DATA_REPLICA_WRITE);
    int replicaRead = conf.get(TezClientConf.RSS_DATA_REPLICA_READ);
    int replica = conf.get(TezClientConf.RSS_DATA_REPLICA);
    boolean replicaSkipEnabled = conf.get(TezClientConf.RSS_DATA_REPLICA_SKIP_ENABLED);
    int dataTransferPoolSize = conf.get(TezClientConf.RSS_DATA_TRANSFER_POOL_SIZE);
    int dataCommitPoolSize = conf.get(TezClientConf.RSS_DATA_COMMIT_POOL_SIZE);
    ShuffleWriteClient client =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(replica)
                    .replicaWrite(replicaWrite)
                    .replicaRead(replicaRead)
                    .replicaSkipEnabled(replicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize));
    return client;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    long initialMemRequestMb =
        conf.getLong(
            TezClientConf.RSS_RUNTIME_IO_SORT_MB.key(),
            TezClientConf.RSS_RUNTIME_IO_SORT_MB.defaultValue());

    LOG.info("InitialMemRequestMb is {}", initialMemRequestMb);
    LOG.info("MaxAvailableTaskMemory is {}", maxAvailableTaskMemory);
    long reqBytes = initialMemRequestMb << 20;
    Preconditions.checkArgument(
        initialMemRequestMb > 0 && reqBytes < maxAvailableTaskMemory,
        TezClientConf.RSS_RUNTIME_IO_SORT_MB.key()
            + initialMemRequestMb
            + " should be "
            + "larger than 0 and should be less than the available task memory (MB):"
            + (maxAvailableTaskMemory >> 20));
    LOG.info(
        "Requested BufferSize ("
            + TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB
            + ") : "
            + initialMemRequestMb);
    return reqBytes;
  }

  public static String uniqueIdentifierToAttemptId(String uniqueIdentifier) {
    if (uniqueIdentifier == null) {
      throw new RssException("uniqueIdentifier should not be null");
    }
    String[] ids = uniqueIdentifier.split("_");
    return StringUtils.join(ids, "_", 0, 7);
  }

  public static long getBlockId(int partitionId, long taskAttemptId, int nextSeqNo) {
    LOG.info(
        "GetBlockId, partitionId:{}, taskAttemptId:{}, nextSeqNo:{}",
        partitionId,
        taskAttemptId,
        nextSeqNo);
    long attemptId = taskAttemptId >> (LAYOUT.partitionIdBits + LAYOUT.taskAttemptIdBits);
    if (attemptId < 0 || attemptId > MAX_ATTEMPT_ID) {
      throw new RssException(
          "Can't support attemptId [" + attemptId + "], the max value should be " + MAX_ATTEMPT_ID);
    }
    if (nextSeqNo < 0 || nextSeqNo > MAX_SEQUENCE_NO) {
      throw new RssException(
          "Can't support sequence [" + nextSeqNo + "], the max value should be " + MAX_SEQUENCE_NO);
    }

    int atomicInt = (int) ((nextSeqNo << MAX_ATTEMPT_LENGTH) + attemptId);
    long taskId =
        taskAttemptId - (attemptId << (LAYOUT.partitionIdBits + LAYOUT.taskAttemptIdBits));

    return LAYOUT.getBlockId(atomicInt, partitionId, taskId);
  }

  public static long getTaskAttemptId(long blockId) {
    int mapId = LAYOUT.getTaskAttemptId(blockId);
    int attemptId = LAYOUT.getSequenceNo(blockId) & MAX_ATTEMPT_ID;
    return LAYOUT.getBlockId(attemptId, 0, mapId);
  }

  public static int estimateTaskConcurrency(TezClientConf conf, int mapNum, int reduceNum) {
    double dynamicFactor = conf.get(TezClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR);
    double slowStart =
        conf.getDouble(Constants.MR_SLOW_START, Constants.MR_SLOW_START_DEFAULT_VALUE);
    int mapLimit = conf.getInteger(Constants.MR_MAP_LIMIT, Constants.MR_MAP_LIMIT_DEFAULT_VALUE);
    int reduceLimit =
        conf.getInteger(Constants.MR_REDUCE_LIMIT, Constants.MR_REDUCE_LIMIT_DEFAULT_VALUE);

    int estimateMapNum = mapLimit > 0 ? Math.min(mapNum, mapLimit) : mapNum;
    int estimateReduceNum = reduceLimit > 0 ? Math.min(reduceNum, reduceLimit) : reduceNum;
    if (slowStart == 1) {
      return (int) (Math.max(estimateMapNum, estimateReduceNum) * dynamicFactor);
    } else {
      return (int) (((1 - slowStart) * estimateMapNum + estimateReduceNum) * dynamicFactor);
    }
  }

  public static int getRequiredShuffleServerNumber(TezClientConf conf, int mapNum, int reduceNum) {
    int requiredShuffleServerNumber =
        conf.get(TezClientConf.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
    boolean enabledEstimateServer = conf.get(TezClientConf.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED);
    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
      return requiredShuffleServerNumber;
    }
    int taskConcurrency = estimateTaskConcurrency(conf, mapNum, reduceNum);
    int taskConcurrencyPerServer = conf.get(TezClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER);
    return (int) Math.ceil(taskConcurrency * 1.0 / taskConcurrencyPerServer);
  }

  /**
   * @param tezDagID Get from tez InputContext, represent dag id.
   * @param upVertexId Up stream vertex id of the task.
   * @param downVertexId The vertex id of task.
   * @return The shuffle id.
   */
  public static int computeShuffleId(int tezDagID, int upVertexId, int downVertexId) {
    int shuffleId =
        tezDagID * (SHUFFLE_ID_MAGIC * SHUFFLE_ID_MAGIC)
            + upVertexId * SHUFFLE_ID_MAGIC
            + downVertexId;
    LOG.info(
        "Compute Shuffle Id:{}, up vertex id:{}, down vertex id:{}",
        shuffleId,
        upVertexId,
        downVertexId);
    return shuffleId;
  }

  public static int parseDagId(int shuffleId) {
    Preconditions.checkArgument(shuffleId > 0, "shuffleId should be positive.");
    int dagId = shuffleId / (SHUFFLE_ID_MAGIC * SHUFFLE_ID_MAGIC);
    if (dagId == 0) {
      throw new RssException("Illegal shuffleId: " + shuffleId);
    }
    return dagId;
  }

  /**
   * @param vertexName: vertex name, like "Map 1" or "Reducer 2"
   * @return Map vertex name of String type to int type. Split vertex name, get vertex type and
   *     vertex id number, if it's map vertex, then return vertex id number, else if it's reducer
   *     vertex, then add VERTEX_ID_MAPPING_MAGIC and vertex id number finally return it.
   */
  private static int mapVertexId(String vertexName) {
    String[] ss = vertexName.split("\\s+");
    if (Integer.parseInt(ss[1]) > VERTEX_ID_MAPPING_MAX_ID) {
      throw new RssException("Too large vertex name to id mapping, vertexName:" + vertexName);
    }
    if (VERTEX_ID_MAPPING_MAP.equals(ss[0])) {
      return Integer.parseInt(ss[1]);
    } else if (VERTEX_ID_MAPPING_REDUCER.equals(ss[0])) {
      return VERTEX_ID_MAPPING_MAGIC + Integer.parseInt(ss[1]);
    } else {
      throw new RssException("Wrong vertex name to id mapping, vertexName:" + vertexName);
    }
  }

  public static long convertTaskAttemptIdToLong(TezTaskAttemptID taskAttemptID) {
    int lowBytes = taskAttemptID.getTaskID().getId();
    if (lowBytes > LAYOUT.maxTaskAttemptId) {
      throw new RssException("TaskAttempt " + taskAttemptID + " low bytes " + lowBytes + " exceed");
    }
    int highBytes = taskAttemptID.getId();
    if (highBytes > MAX_ATTEMPT_ID || highBytes < 0) {
      throw new RssException(
          "TaskAttempt " + taskAttemptID + " high bytes " + highBytes + " exceed.");
    }
    long id = LAYOUT.getBlockId(highBytes, 0, lowBytes);
    LOG.info("ConvertTaskAttemptIdToLong taskAttemptID:{}, id is {}, .", taskAttemptID, id);
    return id;
  }

  public static Roaring64NavigableMap fetchAllRssTaskIds(
      Set<InputAttemptIdentifier> successMapTaskAttempts, int totalMapsCount, int appAttemptId) {
    String errMsg = "TaskAttemptIDs are inconsistent with map tasks";
    Roaring64NavigableMap rssTaskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap mapTaskIdBitmap = Roaring64NavigableMap.bitmapOf();
    LOG.info("FetchAllRssTaskIds successMapTaskAttempts size:{}", successMapTaskAttempts.size());
    LOG.info("FetchAllRssTaskIds totalMapsCount:{}, appAttemptId:{}", totalMapsCount, appAttemptId);

    for (InputAttemptIdentifier inputAttemptIdentifier : successMapTaskAttempts) {
      String pathComponent = inputAttemptIdentifier.getPathComponent();
      TezTaskAttemptID mapTaskAttemptID = IdUtils.convertTezTaskAttemptID(pathComponent);
      long rssTaskId = RssTezUtils.convertTaskAttemptIdToLong(mapTaskAttemptID);
      long mapTaskId = mapTaskAttemptID.getTaskID().getId();

      LOG.info(
          "FetchAllRssTaskIds, pathComponent: {}, mapTaskId:{}, rssTaskId:{}, is contains:{}",
          pathComponent,
          mapTaskId,
          rssTaskId,
          mapTaskIdBitmap.contains(mapTaskId));
      if (!mapTaskIdBitmap.contains(mapTaskId)) {
        rssTaskIdBitmap.addLong(rssTaskId);
        mapTaskIdBitmap.addLong(mapTaskId);
        if (mapTaskId
            >= totalMapsCount) { // up-stream map task index should < total task number(including
          // failed task)
          LOG.warn(
              inputAttemptIdentifier
                  + " has overflowed mapIndex, pathComponent: "
                  + pathComponent
                  + ",totalMapsCount: "
                  + totalMapsCount);
        }
      } else {
        LOG.warn(inputAttemptIdentifier + " is redundant on index: " + mapTaskId);
      }
    }
    // each map should have only one success attempt
    if (mapTaskIdBitmap.getLongCardinality() != rssTaskIdBitmap.getLongCardinality()) {
      throw new IllegalStateException(errMsg);
    }
    return rssTaskIdBitmap;
  }

  public static int taskIdStrToTaskId(String taskIdStr) {
    try {
      int pos1 = taskIdStr.indexOf(UNDERLINE_DELIMITER);
      int pos2 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos1 + 1);
      int pos3 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos2 + 1);
      int pos4 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos3 + 1);
      int pos5 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos4 + 1);
      int pos6 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos5 + 1);
      return Integer.parseInt(taskIdStr.substring(pos5 + 1, pos6));
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Failed to get VertexId, taskId:{}.", taskIdStr, e);
      throw e;
    }
  }

  // multiHostInfo is like:
  // 172.19.193.247:19999+1_4_7, 172.19.193.55:19999+2_5, 172.19.193.152:19999+0_3_6
  private static void parseRssWorkerFromHostInfo(
      Map<Integer, Set<ShuffleServerInfo>> rssWorker, String multiHostInfo) {
    for (String hostInfo : multiHostInfo.split(",")) {
      // LOG.info("ParseRssWorker, hostInfo:{}", hostInfo);
      String[] info = hostInfo.split("\\+");
      ShuffleServerInfo serverInfo =
          new ShuffleServerInfo(info[0].split(":")[0], Integer.parseInt(info[0].split(":")[1]));

      String[] partitions = info[1].split("_");
      assert (partitions.length > 0);
      for (String partitionId : partitions) {
        rssWorker.computeIfAbsent(Integer.parseInt(partitionId), k -> new HashSet<>());
        rssWorker.get(Integer.parseInt(partitionId)).add(serverInfo);
      }
    }
  }

  // hostnameInfo is like:
  // 172.19.193.247:19999+1_4_7, 172.19.193.55:19999+2_5,172.19.193.152:19999+0_3_6
  public static void parseRssWorker(
      Map<Integer, Set<ShuffleServerInfo>> rssWorker, int shuffleId, String hostnameInfo) {
    LOG.info("ParseRssWorker, hostnameInfo length:{}", hostnameInfo.length());
    for (String toVertex : hostnameInfo.split(";")) {
      // toVertex is like:
      // 1001602=172.19.193.247:19999+1_4_7,172.19.193.55:19999+2_5,172.19.193.152:19999+0_3_6
      String[] splits = toVertex.split("=");
      if (splits.length == 2 && String.valueOf(shuffleId).equals(splits[0])) {
        String workerStr = splits[1];
        parseRssWorkerFromHostInfo(rssWorker, workerStr);
      }
    }
  }

  public static String replaceRssOutputClassName(String className) {
    if (className.equals(OrderedPartitionedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssOrderedPartitionedKVOutput.class.getName());
      return RssOrderedPartitionedKVOutput.class.getName();
    } else if (className.equals(UnorderedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssUnorderedKVOutput.class.getName());
      return RssUnorderedKVOutput.class.getName();
    } else if (className.equals(UnorderedPartitionedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssUnorderedPartitionedKVOutput.class.getName());
      return RssUnorderedPartitionedKVOutput.class.getName();
    } else {
      LOG.info("Unexpected kv output class name {}.", className);
      return className;
    }
  }

  public static String replaceRssInputClassName(String className) {
    if (className.equals(OrderedGroupedKVInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssOrderedGroupedKVInput.class.getName());
      return RssOrderedGroupedKVInput.class.getName();
    } else if (className.equals(OrderedGroupedMergedKVInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssOrderedGroupedMergedKVInput.class.getName());
      return RssOrderedGroupedMergedKVInput.class.getName();
    } else if (className.equals(OrderedGroupedInputLegacy.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssOrderedGroupedInputLegacy.class.getName());
      return RssOrderedGroupedInputLegacy.class.getName();
    } else if (className.equals(UnorderedKVInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssUnorderedKVInput.class.getName());
      return RssUnorderedKVInput.class.getName();
    } else if (className.equals(ConcatenatedMergedKeyValueInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssConcatenatedMergedKeyValueInput.class.getName());
      return RssConcatenatedMergedKeyValueInput.class.getName();
    } else if (className.equals(ConcatenatedMergedKeyValuesInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssConcatenatedMergedKeyValuesInput.class.getName());
      return RssConcatenatedMergedKeyValuesInput.class.getName();
    } else {
      LOG.info("Unexpected kv input class name {}.", className);
      return className;
    }
  }

  public static void applyDynamicClientConf(TezClientConf conf, Map<String, String> confItems) {
    if (conf == null) {
      LOG.warn("Tez conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String tezConfKey = kv.getKey();
      if (!tezConfKey.startsWith(TezClientConf.TEZ_RSS_CONFIG_PREFIX)) {
        tezConfKey = TezClientConf.TEZ_RSS_CONFIG_PREFIX + tezConfKey;
      }
      String tezConfVal = kv.getValue();
      if (StringUtils.isEmpty(conf.getString(tezConfKey, ""))
          || TezClientConf.RSS_MANDATORY_CLUSTER_CONF.contains(tezConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", tezConfKey, tezConfVal);
        conf.setString(tezConfKey, tezConfVal);
      }
    }
  }

  public static void applyDynamicClientConf(Configuration conf, Map<String, String> confItems) {
    if (conf == null) {
      LOG.warn("Tez conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String tezConfKey = kv.getKey();
      if (!tezConfKey.startsWith(TezClientConf.TEZ_RSS_CONFIG_PREFIX)) {
        tezConfKey = TezClientConf.TEZ_RSS_CONFIG_PREFIX + tezConfKey;
      }
      String tezConfVal = kv.getValue();
      if (StringUtils.isEmpty(conf.get(tezConfKey, ""))
          || TezClientConf.RSS_MANDATORY_CLUSTER_CONF.contains(tezConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", tezConfKey, tezConfVal);
        conf.set(tezConfKey, tezConfVal);
      }
    }
  }

  public static Configuration filterRssConf(Configuration extraConf) {
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : extraConf) {
      String key = entry.getKey();
      if (key.startsWith(TezClientConf.TEZ_RSS_CONFIG_PREFIX)) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }
}
