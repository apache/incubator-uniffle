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

package org.apache.uniffle.server;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.TestUtils;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopNShuffleDataSizeOfAppCalcTaskTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TopNShuffleDataSizeOfAppCalcTaskTest.class);

  protected static List<ShuffleServer> shuffleServers = Lists.newArrayList();
  private static final Long EVENT_THRESHOLD_SIZE = 2048L;
  protected static final int SHUFFLE_SERVER_PORT = 20001;
  static @TempDir File tempDir;

  protected static final String LOCALHOST;

  static {
    try {
      LOCALHOST = RssUtils.getHostIp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static final int COORDINATOR_PORT_1 = 19999;
  protected static final int NETTY_PORT = 21000;
  protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT_1;

  protected static ShuffleServerConf getShuffleServerConf() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
    serverConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE_HDFS.name());
    serverConf.setString("rss.storage.basePath", tempDir.getAbsolutePath());
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
    serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.coordinator.quorum", COORDINATOR_QUORUM);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 18080);
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
    serverConf.setInteger(ShuffleServerConf.NETTY_SERVER_PORT, NETTY_PORT);
    serverConf.setString("rss.server.tags", "GRPC,GRPC_NETTY");
    return serverConf;
  }

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File dataDir1 = new File(tmpDir, "data1");
    String basePath = dataDir1.getAbsolutePath();
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    shuffleServerConf.set(
        ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, EVENT_THRESHOLD_SIZE);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
    shuffleServerConf.set(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_REFRESH_INTERVAL, 700);
    shuffleServerConf.set(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_SIZE_NUMBER, 5);

    createShuffleServer(shuffleServerConf);
    startServers();
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new ShuffleServer(serverConf));
  }

  public static void startServers() throws Exception {
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.start();
    }
  }

  private void registerAndRequireBuffer(String appId, int length) {
    ShuffleServerGrpcClient shuffleServerClient =
        new ShuffleServerGrpcClient("localhost", SHUFFLE_SERVER_PORT);
    int shuffleId = 0;
    int partitionId = 0;
    List<PartitionRange> partitionIds = Lists.newArrayList(new PartitionRange(0, 3));

    RssRegisterShuffleRequest registerShuffleRequest =
        new RssRegisterShuffleRequest(appId, shuffleId, partitionIds, "");
    RssRegisterShuffleResponse registerResponse =
        shuffleServerClient.registerShuffle(registerShuffleRequest);
    assertSame(StatusCode.SUCCESS, registerResponse.getStatusCode());

    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                shuffleId,
                partitionId,
                0,
                length,
                0,
                new byte[] {},
                Lists.newArrayList(),
                0,
                100,
                0));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    RssSendShuffleDataRequest sendShuffleDataRequest =
        new RssSendShuffleDataRequest(appId, 1, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response =
        shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
  }

  @Test
  public void testTopNShuffleDataSizeOfAppCalcTask() throws Exception {
    // Here is 6 app, but config max top n number is 5
    registerAndRequireBuffer("application_id_1", 1000);
    registerAndRequireBuffer("application_id_2", 2000);
    registerAndRequireBuffer("application_id_3", 3000);
    registerAndRequireBuffer("application_id_4", 4000);
    registerAndRequireBuffer("application_id_5", 5000);
    registerAndRequireBuffer("application_id_6", 6000);

    Thread.sleep(500);
    String content = TestUtils.httpGet("http://127.0.0.1:18080/metrics/server");
    LOG.info(content);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    JsonNode metricsNode = actualObj.get("metrics");
    Set<String> topNTotalDataSizeApps = new HashSet<>();
    Set<String> topNInMemoryDataSizeApps = new HashSet<>();
    for (int i = 0; i < metricsNode.size(); i++) {
      JsonNode metricsName = metricsNode.get(i).get("name");
      if (ShuffleServerMetrics.TOPN_OF_TOTAL_DATA_SIZE_FOR_APP.equals(metricsName.textValue())) {
        Iterator<Map.Entry<String, JsonNode>> it = metricsNode.get(i).fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          if ("labelValues".equalsIgnoreCase(entry.getKey())) {
            topNTotalDataSizeApps.add(entry.getValue().toString());
          }
        }
      }
      if (ShuffleServerMetrics.TOPN_OF_IN_MEMORY_DATA_SIZE_FOR_APP.equals(
          metricsName.textValue())) {
        Iterator<Map.Entry<String, JsonNode>> it = metricsNode.get(i).fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          if ("labelValues".equalsIgnoreCase(entry.getKey())) {
            topNInMemoryDataSizeApps.add(entry.getValue().toString());
          }
        }
      }
    }

    Set<String> expectedTopNApps =
        Sets.newHashSet(
            "[\"application_id_6\"]",
            "[\"application_id_5\"]",
            "[\"application_id_4\"]",
            "[\"application_id_3\"]",
            "[\"application_id_2\"]");
    assertTrue(
        expectedTopNApps.containsAll(topNTotalDataSizeApps)
            && expectedTopNApps.size() == topNTotalDataSizeApps.size());
    assertTrue(
        expectedTopNApps.containsAll(topNInMemoryDataSizeApps)
            && expectedTopNApps.size() == topNInMemoryDataSizeApps.size());
  }

  @AfterAll
  public static void shutdownServers() throws Exception {

    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    ShuffleServerMetrics.clear();
  }
}
