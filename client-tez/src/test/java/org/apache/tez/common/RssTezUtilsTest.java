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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.util.StorageType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RssTezUtilsTest {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

//  @Test
//  public void applyDynamicClientConfTest() {
//    final Configuration conf = new Configuration();
//    Map<String, String> clientConf = Maps.newHashMap();
//    String remoteStoragePath = "hdfs://path1";
//    String mockKey = "mapreduce.mockKey";
//    String mockValue = "v";
//
//    clientConf.put(RssClientConfig.RSS_REMOTE_STORAGE_PATH, remoteStoragePath);
//    clientConf.put(RssClientConfig.RSS_CLIENT_TYPE, RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
//    clientConf.put(RssClientConfig.RSS_CLIENT_RETRY_MAX,
//            Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
//            Long.toString(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_DATA_REPLICA,
//            Integer.toString(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_DATA_REPLICA_WRITE,
//            Integer.toString(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_DATA_REPLICA_READ,
//            Integer.toString(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_HEARTBEAT_INTERVAL,
//            Long.toString(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
//    clientConf.put(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
//            Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
//            Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
//            Integer.toString(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_INDEX_READ_LIMIT,
//            Integer.toString(RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE));
//    clientConf.put(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE,
//            RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
//    clientConf.put(mockKey, mockValue);
//
//    RssTezUtils.applyDynamicClientConf(conf, clientConf);
//    assertEquals(remoteStoragePath, conf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH));
//    assertEquals(RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE,
//            conf.get(RssTezConfig.RSS_CLIENT_TYPE));
//    assertEquals(Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_CLIENT_RETRY_MAX));
//    assertEquals(Long.toString(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX));
//    assertEquals(Integer.toString(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_DATA_REPLICA));
//    assertEquals(Integer.toString(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_DATA_REPLICA_WRITE));
//    assertEquals(Integer.toString(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_DATA_REPLICA_READ));
//    assertEquals(Long.toString(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_HEARTBEAT_INTERVAL));
//    assertEquals(StorageType.MEMORY_LOCALFILE_HDFS.name(), conf.get(RssTezConfig.RSS_STORAGE_TYPE));
//    assertEquals(Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS));
//    assertEquals(Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS));
//    assertEquals(Integer.toString(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_PARTITION_NUM_PER_RANGE));
//    assertEquals(Integer.toString(RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_INDEX_READ_LIMIT));
//    assertEquals(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE,
//            conf.get(RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE));
//    assertEquals(mockValue, conf.get(mockKey));
//
//    String remoteStoragePath2 = "hdfs://path2";
//    clientConf = Maps.newHashMap();
//    clientConf.put(RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_HDFS.name());
//    clientConf.put(RssTezConfig.RSS_REMOTE_STORAGE_PATH, remoteStoragePath2);
//    clientConf.put(mockKey, "won't be rewrite");
//    clientConf.put(RssClientConfig.RSS_CLIENT_RETRY_MAX, "99999");
//    RssTezUtils.applyDynamicClientConf(conf, clientConf);
//    // overwrite
//    assertEquals(remoteStoragePath2, conf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH));
//    assertEquals(StorageType.MEMORY_HDFS.name(), conf.get(RssTezConfig.RSS_STORAGE_TYPE));
//    // won't be overwrite
//    assertEquals(mockValue, conf.get(mockKey));
//    assertEquals(Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE),
//            conf.get(RssTezConfig.RSS_CLIENT_RETRY_MAX));
//  }

  @Test
  public void testEstimateTaskConcurrency() {
    Configuration jobConf = new Configuration();
    int mapNum = 500;
    int reduceNum = 20;
    assertEquals(495, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));

    jobConf.setDouble(Constants.MR_SLOW_START, 1.0);
    assertEquals(500, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));
    jobConf.setInt(Constants.MR_MAP_LIMIT, 200);
    jobConf.setInt(Constants.MR_REDUCE_LIMIT, 200);
    assertEquals(200, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));

    jobConf.setDouble("mapreduce.rss.estimate.task.concurrency.dynamic.factor", 0.5);
    assertEquals(200, RssTezUtils.estimateTaskConcurrency(jobConf,mapNum, reduceNum));
  }

  @Test
  public void testGetRequiredShuffleServerNumber() {
    Configuration jobConf = new Configuration();
    int mapNum = 500;
    int reduceNum = 20;
    jobConf.setInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, 10);
    assertEquals(10, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));

    jobConf.setBoolean(RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED, true);
    assertEquals(10, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));

    jobConf.unset(RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
    assertEquals(7, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));

    jobConf.setDouble(Constants.MR_SLOW_START, 1.0);
    assertEquals(7, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));

    jobConf.setInt(Constants.MR_MAP_LIMIT, 200);
    jobConf.setInt(Constants.MR_REDUCE_LIMIT, 200);
    assertEquals(3, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));

    jobConf.setDouble("mapreduce.rss.estimate.task.concurrency.dynamic.factor", 0.5);
    assertEquals(3, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
  }

  @Test
  public void testValidateRssClientConf() {
    Configuration jobConf = new Configuration();
    Configuration rssJobConf = new Configuration();
    rssJobConf.setInt("mapreduce.job.maps", 500);
    rssJobConf.setInt("mapreduce.job.reduces", 20);
    RssTezUtils.validateRssClientConf(rssJobConf, jobConf);
    rssJobConf.setInt(RssTezConfig.RSS_CLIENT_RETRY_MAX, 5);
    rssJobConf.setLong(RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX, 1000L);
    rssJobConf.setLong(RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS, 4999L);
    try {
      RssTezUtils.validateRssClientConf(rssJobConf, jobConf);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should not bigger than"));
    }
  }

  @Test
  public void blockConvertTest() {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, 2);
    long taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(taId, 1);
    long blockId = RssTezUtils.getBlockId(1, taskAttemptId, 0);
    long newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
    blockId = RssTezUtils.getBlockId(2, taskAttemptId, 2);
    newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
  }

  @Test
  public void attemptTaskIdTest() {
    String tezTaskAttemptId = "attempt_1677051234358_0091_1_00_000000_0";
    TezTaskAttemptID originalTezTaskAttemptID = TezTaskAttemptID.fromString(tezTaskAttemptId);
    String uniqueIdentifier = String.format("%s_%05d", tezTaskAttemptId, 3);
    String uniqueIdentifierToAttemptId = RssTezUtils.uniqueIdentifierToAttemptId(uniqueIdentifier);
    assertEquals(tezTaskAttemptId, uniqueIdentifierToAttemptId);
    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.fromString(uniqueIdentifierToAttemptId);
    assertEquals(originalTezTaskAttemptID, tezTaskAttemptID);
  }

}
