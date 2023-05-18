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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;

public class RssTezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssTezUtils.class);

  private static final int MAX_ATTEMPT_LENGTH = 6;
  private static final long MAX_ATTEMPT_ID = (1 << MAX_ATTEMPT_LENGTH) - 1;

  public static final String HOST_NAME = "hostname";

  public static final String PLUS_DELIMITER = "+";
  public static final String UNDERLINE_DELIMITER = "_";
  public static final String COLON_DELIMITER = ":";
  public static final String COMMA_DELIMITER = ",";

  // constant to compute shuffle id
  private static final int VERTEX_ID_MAPPING_MAX_ID = 500;
  private static final String VERTEX_ID_MAPPING_MAP = "Map";
  private static final String VERTEX_ID_MAPPING_REDUCER = "Reducer";
  private static final int VERTEX_ID_MAPPING_MAGIC = 600;
  private static final int SHUFFLE_ID_MAGIC = 1000;



  private RssTezUtils() {
  }

  public static ShuffleWriteClient createShuffleClient(Configuration conf) {
    int heartBeatThreadNum = conf.getInt(RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
        RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax = conf.getInt(RssTezConfig.RSS_CLIENT_RETRY_MAX,
        RssTezConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = conf.getLong(RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String clientType = conf.get(RssTezConfig.RSS_CLIENT_TYPE,
        RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int replicaWrite = conf.getInt(RssTezConfig.RSS_DATA_REPLICA_WRITE,
        RssTezConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead = conf.getInt(RssTezConfig.RSS_DATA_REPLICA_READ,
        RssTezConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    int replica = conf.getInt(RssTezConfig.RSS_DATA_REPLICA,
        RssTezConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    boolean replicaSkipEnabled = conf.getBoolean(RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED,
        RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);
    int dataTransferPoolSize = conf.getInt(RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE,
        RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE);
    int dataCommitPoolSize = conf.getInt(RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE,
        RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);
    ShuffleWriteClient client = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax,
            heartBeatThreadNum, replica, replicaWrite, replicaRead, replicaSkipEnabled,
            dataTransferPoolSize, dataCommitPoolSize);
    return client;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    long initialMemRequestMb = conf.getLong(RssTezConfig.RSS_RUNTIME_IO_SORT_MB,
        RssTezConfig.RSS_DEFAULT_RUNTIME_IO_SORT_MB);
    LOG.info("InitialMemRequestMb is {}", initialMemRequestMb);
    LOG.info("MaxAvailableTaskMemory is {}", maxAvailableTaskMemory);
    long reqBytes = initialMemRequestMb << 20;
    Preconditions.checkArgument(initialMemRequestMb > 0 && reqBytes < maxAvailableTaskMemory,
            RssTezConfig.RSS_RUNTIME_IO_SORT_MB + initialMemRequestMb
                    + " should be " + "larger than 0 and should be less than the available task memory (MB):"
                + (maxAvailableTaskMemory >> 20));
    LOG.info("Requested BufferSize (" + TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB
            + ") : " + initialMemRequestMb);
    return reqBytes;
  }

  public static String uniformPartitionHostInfo(Map<Integer, List<ShuffleServerInfo>> map) {
    List<String> res = new ArrayList<>();
    String tmp;
    Set<Integer> pidSet = map.keySet();
    for (Integer pid : pidSet) {
      for (ShuffleServerInfo shuffleServerInfo : map.get(pid)) {
        tmp = pid + UNDERLINE_DELIMITER + shuffleServerInfo.getHost() + COLON_DELIMITER + shuffleServerInfo.getNettyPort();
        res.add(tmp);
      }
    }
    return org.apache.commons.lang.StringUtils.join(res, COMMA_DELIMITER);
  }

  public static Map<String, List<String>> uniformServerToPartitions(String partitionToServers) {
    Map<String, List<String>> serverToPartitions = new HashMap<>();
    List<String> list;

    String[] pidWithWorkerInfos = partitionToServers.split(COMMA_DELIMITER);
    for (String pidWithWorkerInfo : pidWithWorkerInfos) {
      String[] pidUnderLineWorkerInfo = pidWithWorkerInfo.split(UNDERLINE_DELIMITER);
      if (serverToPartitions.containsKey(pidUnderLineWorkerInfo[1])) {
        list = serverToPartitions.get(pidUnderLineWorkerInfo[1]);
        list.add(pidUnderLineWorkerInfo[0]);
      } else {
        list = new ArrayList<>();
        list.add(pidUnderLineWorkerInfo[0]);
        serverToPartitions.put(pidUnderLineWorkerInfo[1], list);
      }
    }

    return serverToPartitions;
  }

  public static String uniformServerToPartitions(Map<String, List<String>> map) {
    List<String> res = new ArrayList<>();
    Set<String> keySet = map.keySet();
    for (String s : keySet) {
      String join = org.apache.commons.lang.StringUtils.join(map.get(s), UNDERLINE_DELIMITER);
      res.add(s + PLUS_DELIMITER + join);
    }

    return org.apache.commons.lang.StringUtils.join(res,COMMA_DELIMITER);
  }

  public static ApplicationAttemptId getApplicationAttemptId() {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    return containerId.getApplicationAttemptId();
  }

  public static String uniqueIdentifierToAttemptId(String uniqueIdentifier) {
    if (uniqueIdentifier == null) {
      throw new RuntimeException("uniqueIdentifier should not be null");
    }
    String[] ids = uniqueIdentifier.split("_");
    return StringUtils.join(ids, "_", 0, 7);
  }

  public static int getInt(Configuration rssJobConf, Configuration mrJobCOnf, String key, int defaultValue) {
    return rssJobConf.getInt(key,  mrJobCOnf.getInt(key, defaultValue));
  }

  public static long getLong(Configuration rssJobConf, Configuration mrJobConf, String key, long defaultValue) {
    return rssJobConf.getLong(key, mrJobConf.getLong(key, defaultValue));
  }

  public static boolean getBoolean(Configuration rssJobConf, Configuration mrJobConf, String key,
                                   boolean defaultValue) {
    return rssJobConf.getBoolean(key, mrJobConf.getBoolean(key, defaultValue));
  }

  public static double getDouble(Configuration rssJobConf, Configuration mrJobConf, String key, double defaultValue) {
    return rssJobConf.getDouble(key, mrJobConf.getDouble(key, defaultValue));
  }

  public static String getString(Configuration rssJobConf, Configuration mrJobConf, String key) {
    return rssJobConf.get(key, mrJobConf.get(key));
  }

  public static String getString(Configuration rssJobConf, Configuration mrJobConf, String key, String defaultValue) {
    return rssJobConf.get(key, mrJobConf.get(key, defaultValue));
  }

  public static long getBlockId(long partitionId, long taskAttemptId, int nextSeqNo) {
    long attemptId = taskAttemptId >> (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH);
    if (attemptId < 0 || attemptId > MAX_ATTEMPT_ID) {
      throw new RuntimeException("Can't support attemptId [" + attemptId
          + "], the max value should be " + MAX_ATTEMPT_ID);
    }
    long atomicInt = (nextSeqNo << MAX_ATTEMPT_LENGTH) + attemptId;
    if (atomicInt < 0 || atomicInt > Constants.MAX_SEQUENCE_NO) {
      throw new RuntimeException("Can't support sequence [" + atomicInt
          + "], the max value should be " + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new RuntimeException("Can't support partitionId["
          + partitionId + "], the max value should be " + Constants.MAX_PARTITION_ID);
    }
    long taskId = taskAttemptId - (attemptId
        << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH));
    if (taskId < 0 ||  taskId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RuntimeException("Can't support taskId["
          + taskId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskId;
  }

  public static long getTaskAttemptId(long blockId) {
    long mapId = blockId & Constants.MAX_TASK_ATTEMPT_ID;
    long attemptId = (blockId >> (Constants.TASK_ATTEMPT_ID_MAX_LENGTH + Constants.PARTITION_ID_MAX_LENGTH))
        & MAX_ATTEMPT_ID;
    return (attemptId << (Constants.TASK_ATTEMPT_ID_MAX_LENGTH + Constants.PARTITION_ID_MAX_LENGTH)) + mapId;
  }

  public static int estimateTaskConcurrency(Configuration jobConf, int mapNum, int reduceNum) {
    double dynamicFactor = jobConf.getDouble(RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR,
        RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE);
    double slowStart = jobConf.getDouble(Constants.MR_SLOW_START, Constants.MR_SLOW_START_DEFAULT_VALUE);
    int mapLimit = jobConf.getInt(Constants.MR_MAP_LIMIT, Constants.MR_MAP_LIMIT_DEFAULT_VALUE);
    int reduceLimit = jobConf.getInt(Constants.MR_REDUCE_LIMIT, Constants.MR_REDUCE_LIMIT_DEFAULT_VALUE);

    int estimateMapNum = mapLimit > 0 ? Math.min(mapNum, mapLimit) : mapNum;
    int estimateReduceNum = reduceLimit > 0 ? Math.min(reduceNum, reduceLimit) : reduceNum;
    if (slowStart == 1) {
      return (int) (Math.max(estimateMapNum, estimateReduceNum) * dynamicFactor);
    } else {
      return (int) (((1 - slowStart) * estimateMapNum + estimateReduceNum) * dynamicFactor);
    }
  }

  public static int getRequiredShuffleServerNumber(Configuration jobConf, int mapNum, int reduceNum) {
    int requiredShuffleServerNumber = jobConf.getInt(
        RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
        RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE
    );
    boolean enabledEstimateServer = jobConf.getBoolean(
        RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED,
        RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE
    );
    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
      return requiredShuffleServerNumber;
    }
    int taskConcurrency = estimateTaskConcurrency(jobConf, mapNum, reduceNum);
    int taskConcurrencyPerServer = jobConf.getInt(RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER,
        RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE);
    return (int) Math.ceil(taskConcurrency * 1.0 / taskConcurrencyPerServer);
  }

  public static void validateRssClientConf(Configuration rssJobConf, Configuration mrJobConf) {
    int retryMax = getInt(rssJobConf, mrJobConf, RssTezConfig.RSS_CLIENT_RETRY_MAX,
        RssTezConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = getLong(rssJobConf, mrJobConf, RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    long sendCheckTimeout = getLong(rssJobConf, mrJobConf, RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
        RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    if (retryIntervalMax * retryMax > sendCheckTimeout) {
      throw new IllegalArgumentException(String.format("%s(%s) * %s(%s) should not bigger than %s(%s)",
          RssTezConfig.RSS_CLIENT_RETRY_MAX,
          retryMax,
          RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
          retryIntervalMax,
          RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
          sendCheckTimeout));
    }
  }

  // compute shuffle id using
  public static int computeShuffleId(InputContext inputContext) {
    int dagIdentifier = inputContext.getDagIdentifier();
    String sourceVertexName = inputContext.getSourceVertexName();
    String taskVertexName  = inputContext.getTaskVertexName();
    return RssTezUtils.computeShuffleId(dagIdentifier, sourceVertexName, taskVertexName);
  }

  /**
   *
   * @param tezDagID Get from tez InputContext, represent dag id.
   * @param upVertexName Up stream vertex name of the task, like "Map 1" or "Reducer 2".
   * @param downVertexName The vertex name of task, like "Map 1" or "Reducer 2".
   * @return The shuffle id. First convert upVertexName of String type to int, by invoke mapVertexId() method,
   * Then convert downVertexName of String type to int, by invoke mapVertexId() method.
   * Finally compute shuffle id by pass tezDagID, upVertexId, downVertexId and invoke computeShuffleId() method.
   * By map vertex name of String type to int type, we can compute shuffle id.
   */
  public static int computeShuffleId(int tezDagID, String upVertexName, String downVertexName) {
    int upVertexId = mapVertexId(upVertexName);
    int downVertexId = mapVertexId(downVertexName);
    int shuffleId = computeShuffleId(tezDagID, upVertexId, downVertexId);
    LOG.info("Compute Shuffle Id, upVertexName:{}, id:{}, downVertexName:{}, id:{}, shuffleId:{}",
        upVertexName, upVertexId, downVertexName, downVertexId, shuffleId);
    return shuffleId;
  }



  private static int computeShuffleId(int tezDagID, int upTezVertexID, int downTezVertexID) {
    return tezDagID * (SHUFFLE_ID_MAGIC * SHUFFLE_ID_MAGIC)  + upTezVertexID * SHUFFLE_ID_MAGIC + downTezVertexID;
  }

  /**
   *
   * @param vertexName: vertex name, like "Map 1" or "Reducer 2"
   * @return Map vertex name of String type to int type.
   * Split vertex name, get vertex type and vertex id number, if it's map vertex, then return vertex id number,
   * else if it's reducer vertex, then add VERTEX_ID_MAPPING_MAGIC and vertex id number finally return it.
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

  public static long convertTaskAttemptIdToLong(TezTaskAttemptID taskAttemptID, int appAttemptId) {
    long lowBytes = taskAttemptID.getTaskID().getId();
    if (lowBytes > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RssException("TaskAttempt " + taskAttemptID + " low bytes " + lowBytes + " exceed");
    }
    if (appAttemptId < 1) {
      throw new RssException("appAttemptId  " + appAttemptId + " is wrong");
    }
    long highBytes = (long)taskAttemptID.getId() - (appAttemptId - 1) * 1000;
    if (highBytes > MAX_ATTEMPT_ID || highBytes < 0) {
      throw new RssException("TaskAttempt " + taskAttemptID + " high bytes " + highBytes
          + " exceed, appAttemptId:" + appAttemptId);
    }

    long id = (highBytes << (Constants.TASK_ATTEMPT_ID_MAX_LENGTH + Constants.PARTITION_ID_MAX_LENGTH)) + lowBytes;
    LOG.info("convertTaskAttemptIdToLong id is {}", id);
    LOG.info("lowBytes id is {}", lowBytes);
    LOG.info("highBytes id is {}", highBytes);
    return id;
  }

  public static Roaring64NavigableMap fetchAllRssTaskIds(Set<InputAttemptIdentifier> successMapTaskAttempts,
                                                         Integer totalMapsCount, Integer appAttemptId) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap mapIndexBitmap = Roaring64NavigableMap.bitmapOf();
    String errMsg = "TaskAttemptIDs are inconsistent with map tasks";
    LOG.info("FetchAllRssTaskIds successMapTaskAttempts:{}", successMapTaskAttempts);
    LOG.info("FetchAllRssTaskIds totalMapsCount:{}, appAttemptId:{}", totalMapsCount, appAttemptId);

    for (InputAttemptIdentifier inputAttemptIdentifier: successMapTaskAttempts) {
      int mapIndex = inputAttemptIdentifier.getInputIdentifier();
      String pathComponent = inputAttemptIdentifier.getPathComponent();
      int taskId = RssTezUtils.taskIdStrToTaskId(pathComponent);
      // There can be multiple successful attempts on same map task.
      // So we only need to accept one of them.
      LOG.info("FetchAllRssTaskIds, taskId:{}, is contains:{}", taskId, mapIndexBitmap.contains(taskId));
      if (!mapIndexBitmap.contains(taskId)) {
        taskIdBitmap.addLong(taskId);

        LOG.info("FetchAllRssTaskIds, successMapTaskAttempts:{}, totalMapsCount:{}, appAttemptId:{}, mapIndex:{}, "
                + "totalMapsCount:{}, taskId: {} ",
            successMapTaskAttempts.size(), totalMapsCount, appAttemptId, mapIndex, totalMapsCount, taskId);

        if (mapIndex < totalMapsCount) {  // up-stream map task index should < total task number(including failed task)
          mapIndexBitmap.addLong(taskId);
        } else {

          LOG.error("SuccessMapTaskAttempts:{}, totalMapsCount:{}, appAttemptId:{}, mapIndex:{}, totalMapsCount:{} ",
              successMapTaskAttempts, totalMapsCount, appAttemptId, mapIndex, totalMapsCount);

          LOG.error(inputAttemptIdentifier + " has overflowed mapIndex");
          throw new IllegalStateException(errMsg);
        }
      } else {
        LOG.warn(inputAttemptIdentifier + " is redundant on index: " + mapIndex);
      }
    }
    // each map should have only one success attempt
    if (mapIndexBitmap.getLongCardinality() != taskIdBitmap.getLongCardinality()) {
      throw new IllegalStateException(errMsg);
    }

    return taskIdBitmap;
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
      LOG.error("Failed to get VertexId, taskId:{}.",taskIdStr, e);
      throw e;
    }
  }

  // multiHostInfo is like:
  // 0_172.19.193.152:19999,1_172.19.193.152:19999
  private static void parseRssWorkerFromHostInfo(Map<Integer, Set<ShuffleServerInfo>> rssWorker, String multiHostInfo) {
    for (String hostInfo : multiHostInfo.split(",")) {
      LOG.info("ParseRssWorker, hostInfo:{}", hostInfo);
      String[] info = hostInfo.split("_|:");
      int partitionId = Integer.parseInt(info[0]);
      ShuffleServerInfo serverInfo = new ShuffleServerInfo(info[1], Integer.parseInt(info[2]));
      rssWorker.computeIfAbsent(partitionId, k -> new HashSet<>());
      rssWorker.get(partitionId).add(serverInfo);
      LOG.info("Parse Rss Worker, add partition:{}, serverInfo:{}", partitionId, serverInfo);
    }
  }

  // hostnameInfo is like:
  // Map 1=0_172.19.193.152:19999,0_172.19.193.152:19999;Map 2=0_172.19.193.152:19999,1_17
  public static void parseRssWorker(Map<Integer, Set<ShuffleServerInfo>> rssWorker, int shuffleId,
                                    String hostnameInfo) {
    LOG.info("ParseRssWorker, hostnameInfo:{}", hostnameInfo);
    for (String toVertex: hostnameInfo.split(";")) {
      LOG.info("ParseRssWorker, hostnameInffdafdso:{}", toVertex);
      String[] splits = toVertex.split("=");
      if (splits.length == 2 && String.valueOf(shuffleId).equals(splits[0])) {
        String workerStr = toVertex.split("=")[1];
        LOG.info("ParseRssWorker, workerStr:{}", workerStr);
        parseRssWorkerFromHostInfo(rssWorker, workerStr);
      }
    }
  }
}
