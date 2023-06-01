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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezUtilsTest {

  @Test
  public void baskAttemptIdTest() {
    long taskAttemptId = 0x1000ad12;
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID taskId = TezTaskID.getInstance(vId, (int)taskAttemptId);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(taskId, 3);

    boolean isException = false;
    try {
      RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptId, 1);
    } catch (RssException e) {
      isException = true;
    }
    assertTrue(isException);

    taskId = TezTaskID.getInstance(vId, (int)(1 << 21));
    tezTaskAttemptId = TezTaskAttemptID.getInstance(taskId, 2);
    isException = false;
    try {
      RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptId, 1);
    } catch (RssException e) {
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void blockConvertTest() {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tId, 2);
    long taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptId, 1);
    long blockId = RssTezUtils.getBlockId(1, taskAttemptId, 0);
    long newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
    blockId = RssTezUtils.getBlockId(2, taskAttemptId, 2);
    newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
  }

  @Test
  public void testPartitionIdConvertBlock() {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tId, 2);
    long taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptId, 1);
    long mask = (1L << Constants.PARTITION_ID_MAX_LENGTH) - 1;
    for (int partitionId = 0; partitionId <= 3000; partitionId++) {
      for (int seqNo = 0; seqNo <= 10; seqNo++) {
        long blockId = RssTezUtils.getBlockId(Long.valueOf(partitionId), taskAttemptId, seqNo);
        int newPartitionId = Math.toIntExact((blockId >> Constants.TASK_ATTEMPT_ID_MAX_LENGTH) & mask);
        assertEquals(partitionId, newPartitionId);
      }
    }
  }

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
  public void testComputeShuffleId() {
    int dagId = 1;
    String upVertexName = "Map 1";
    String downVertexName = "Reducer 2";
    assertEquals(1001602, RssTezUtils.computeShuffleId(dagId, upVertexName, downVertexName));
  }

  @Test
  public void testTaskIdStrToTaskId() {
    assertEquals(0, RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
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

  @Test
  public void testParseRssWorker() {
    Map<Integer, Set<ShuffleServerInfo>> rssWorker = new HashMap<>();
    int shuffleId = 1001602;
    // 0_1_2_3 is consist of partition id.
    String hostnameInfo = "localhost;1001602=172.19.193.152:19999+0_1_2_3,172.19.193.153:19999+2_3_4_5";
    RssTezUtils.parseRssWorker(rssWorker, shuffleId, hostnameInfo);

    assertEquals(6, rssWorker.size());

    Set<ShuffleServerInfo> shuffleServerInfo = rssWorker.get(0);
    ShuffleServerInfo server = new ShuffleServerInfo("172.19.193.152", 19999);
    assertEquals(ImmutableSet.of(server), shuffleServerInfo);

    shuffleServerInfo = rssWorker.get(3);
    ShuffleServerInfo server2 = new ShuffleServerInfo("172.19.193.153", 19999);
    assertEquals(ImmutableSet.of(server, server2), shuffleServerInfo);

    shuffleServerInfo = rssWorker.get(18);
    assertNull(shuffleServerInfo);
  }
}
