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

package org.apache.uniffle.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.TestUtils;
import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.server.MockedShuffleServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleServerFaultToleranceTest extends ShuffleReadWriteBase {

  private List<ShuffleServerClient> shuffleServerClients;

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    shuffleServers.add(createServer(0));
    shuffleServers.add(createServer(1));
    shuffleServers.add(createServer(2));
    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClients = new ArrayList<>();
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServerClients.add(new ShuffleServerGrpcClient(shuffleServer.getIp(), shuffleServer.getPort()));
    }
  }

  @AfterEach
  public void cleanEnv() throws Exception {
    shuffleServerClients.forEach((client) -> {
      client.close();
    });
    cleanCluster();
    setupServers();
  }

  @Test
  public void testReadFaultTolerance() throws Exception {
    String testAppId = "ShuffleServerFaultToleranceTest.testReadFaultTolerance";
    int shuffleId = 0;
    int partitionId = 0;

    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)), "");
    registerShuffle(rrsr);
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Long, byte[]> dataMap = Maps.newHashMap();
    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[1];
    bitmaps[0] = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        shuffleId, partitionId, 0, 3, 25,
        expectBlockIds, dataMap, mockSSI);


    RssSendShuffleDataRequest rssdr = getRssSendShuffleDataRequest(testAppId, shuffleId, partitionId, blocks);
    shuffleServerClients.get(1).sendShuffleData(rssdr);
    shuffleServerClients.get(2).sendShuffleData(rssdr);

    List<ShuffleServerInfo> shuffleServerInfoList = new ArrayList<>();
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServerInfoList.add(new ShuffleServerInfo(shuffleServer.getId(),
          shuffleServer.getIp(), shuffleServer.getPort()));
    }
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setStorageType(StorageType.MEMORY_LOCALFILE.name());
    request.setAppId(testAppId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setIndexReadLimit(100);
    request.setPartitionNumPerRange(1);
    request.setPartitionNum(1);
    request.setReadBufferSize(14 * 1024 * 1024);
    request.setShuffleServerInfoList(shuffleServerInfoList);
    request.setExpectBlockIds(expectBlockIds);
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    request.setProcessBlockIds(processBlockIds);
    request.setDistributionType(ShuffleDataDistributionType.NORMAL);
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    request.setExpectTaskIds(taskIdBitmap);
    ClientReadHandler clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.clear();
    expectedData.put(blocks.get(0).getBlockId(), blocks.get(0).getData());
    expectedData.put(blocks.get(1).getBlockId(), blocks.get(1).getData());
    expectedData.put(blocks.get(2).getBlockId(), blocks.get(1).getData());
    ShuffleDataResult sdr  = clientReadHandler.readShuffleData();
    TestUtils.validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks.get(0).getBlockId());
    processBlockIds.addLong(blocks.get(1).getBlockId());
    processBlockIds.addLong(blocks.get(2).getBlockId());

    // send data to shuffle server, and wait until flush finish
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        shuffleId, partitionId, 0, 3, 50,
        expectBlockIds, dataMap, mockSSI);
    rssdr = getRssSendShuffleDataRequest(testAppId, shuffleId, partitionId, blocks2);
    shuffleServerClients.get(1).sendShuffleData(rssdr);
    shuffleServerClients.get(2).sendShuffleData(rssdr);
    RssSendCommitRequest commitRequest = new RssSendCommitRequest(testAppId, shuffleId);
    shuffleServerClients.get(1).sendCommit(commitRequest);
    shuffleServerClients.get(2).sendCommit(commitRequest);

    sdr = clientReadHandler.readShuffleData();
    long blockCount = sdr.getBufferSegments().stream().filter(bufferSegment ->
        !processBlockIds.contains(bufferSegment.getBlockId())).count();
    assertEquals(3, blockCount);
  }

  private RssSendShuffleDataRequest getRssSendShuffleDataRequest(
      String appId, int shuffleId, int partitionId, List<ShuffleBlockInfo> blocks) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);
    return new RssSendShuffleDataRequest(
        appId, 3, 1000, shuffleToBlocks);
  }

  private void registerShuffle(RssRegisterShuffleRequest rrsr) {
    shuffleServerClients.forEach((client) -> {
      client.registerShuffle(rrsr);
    });
  }

  public static MockedShuffleServer createServer(int id) throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 40.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 500L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.DISK_CAPACITY, 1000000L);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, id + "_1");
    File dataDir2 = new File(tmpDir, id + "_2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + id);
    shuffleServerConf.setInteger("rss.jetty.http.port", 19081 + id * 100);
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    return new MockedShuffleServer(shuffleServerConf);
  }

  public static void cleanCluster() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.stopServer();
    }
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    coordinators = Lists.newArrayList();
  }
}
