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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RiffleShuffleServerGrpcTest extends RiffleIntegrationTestBase {

  protected static final long FAILED_REQUIRE_ID = -1;

  private final AtomicInteger atomicInteger = new AtomicInteger(0);

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    createCoordinatorServer(coordinatorConf);
    RiffleShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.set("app_heartbeat_timeout_min", 1);
    shuffleServerConf.set("huge_partition_marked_threshold", "10485760");
    shuffleServerConf.set("huge_partition_memory_max_used_percent", 0.15f);

    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @Test
  public void clearResourceTest() throws Exception {
    final ShuffleWriteClient shuffleWriteClient =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType("GRPC")
                    .retryMax(2)
                    .retryIntervalMax(10000L)
                    .heartBeatThreadNum(4)
                    .replica(1)
                    .replicaWrite(1)
                    .replicaRead(1)
                    .replicaSkipEnabled(true)
                    .dataTransferPoolSize(1)
                    .dataCommitPoolSize(1)
                    .unregisterThreadPoolSize(10)
                    .unregisterRequestTimeSec(10));
    shuffleWriteClient.registerCoordinators("127.0.0.1:9999");
    shuffleWriteClient.registerShuffle(
        new ShuffleServerInfo("127.0.0.1-20001", "127.0.0.1", 19999),
        "application_clearResourceTest1",
        0,
        Lists.newArrayList(new PartitionRange(0, 1)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    shuffleWriteClient.registerApplicationInfo("application_clearResourceTest1", 500L, "user");
    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest1", 500L);
    shuffleWriteClient.registerApplicationInfo("application_clearResourceTest2", 500L, "user");
    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest2", 500L);

    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            "application_clearResourceTest1", 0, Lists.newArrayList(new PartitionRange(0, 1)), "");
    shuffleServerClient.registerShuffle(rrsr);
    rrsr =
        new RssRegisterShuffleRequest(
            "application_clearResourceTest2", 0, Lists.newArrayList(new PartitionRange(0, 1)), "");
    shuffleServerClient.registerShuffle(rrsr);

    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest1", 500L);
    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest2", 500L);

    // Thread will keep refresh clearResourceTest1 in coordinator
    Thread t =
        new Thread(
            () -> {
              int i = 0;
              while (i < 20) {
                shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest1", 500L);
                i++;
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    t.start();

    // Heartbeat is sent to coordinator
    Thread.sleep(3000);
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "application_clearResourceTest1", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    assertEquals(
        Sets.newHashSet("application_clearResourceTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    // clearResourceTest2 will be removed because of rss.server.app.expired.withoutHeartbeat
    Thread.sleep(120000);

    // clearResourceTest1 will be removed because of rss.server.app.expired.withoutHeartbeat
    t.interrupt();

    RssGetShuffleResultRequest req =
        new RssGetShuffleResultRequest("application_clearResourceTest1", 0, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't get shuffle result"));
    }

    req = new RssGetShuffleResultRequest("application_clearResourceTest2", 0, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't get shuffle result"));
    }
  }

  @Test
  public void registerTest() {
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "registerTest", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("registerTest", 0, 0);

    // no exception with getShuffleResult means register successfully
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 0, 1);
    shuffleServerClient.getShuffleResult(req);
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "registerTest",
            1,
            Lists.newArrayList(
                new PartitionRange(0, 0), new PartitionRange(1, 1), new PartitionRange(2, 2)),
            ""));
    req = new RssGetShuffleResultRequest("registerTest", 1, 0);
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 1);
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 2);
    shuffleServerClient.getShuffleResult(req);
  }

  @Test
  public void shuffleResultTest() throws Exception {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    List<Long> blockIds1 = getBlockIdList(1, 3);
    List<Long> blockIds2 = getBlockIdList(2, 2);
    List<Long> blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);

    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds, 1);
    try {
      shuffleServerClient.reportShuffleResult(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't report shuffle result to"));
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't get shuffle result from"));
    }

    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            "shuffleResultTest", 100, Lists.newArrayList(new PartitionRange(0, 1)), "");
    shuffleServerClient.registerShuffle(rrsr);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds, 1);
    RssReportShuffleResultResponse response = shuffleServerClient.reportShuffleResult(request);
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList(1, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 0, 1L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 1, 1L, Maps.newHashMap(), 1);
    shuffleServerClient.reportShuffleResult(request);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    // test with bitmapNum > 1
    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList(1, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);
    request = new RssReportShuffleResultRequest("shuffleResultTest", 2, 1L, partitionToBlockIds, 3);
    shuffleServerClient.reportShuffleResult(request);

    // validate bitmap
    int bitmapLength = 0;
    for (int i = 1; i <= 3; i++) {
      bitmapLength +=
          shuffleServerClient
                      .getShuffleResult(new RssGetShuffleResultRequest("shuffleResultTest", 2, 1))
                      .getBlockIdBitmap()
                  == null
              ? 0
              : 1;
    }
    assertEquals(3, bitmapLength);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList((int) Constants.MAX_PARTITION_ID, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put((int) Constants.MAX_PARTITION_ID, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);
    // bimapNum = 2
    request = new RssReportShuffleResultRequest("shuffleResultTest", 4, 1L, partitionToBlockIds, 2);
    shuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, (int) Constants.MAX_PARTITION_ID);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    // wait resources are deleted
    // rust server will check heart beat every 120 seconds
    Thread.sleep(120000);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't get shuffle result"));
    }
  }

  @Test
  public void sendDataAndRequireBufferTest() throws IOException {
    String appId = "sendDataAndRequireBufferTest";
    int shuffleId = 0;
    int partitionId = 1;
    // bigger than the config above: HUGE_PARTITION_SIZE_THRESHOLD : 1024 * 1024 * 10L
    int hugePartitionDataLength = 1024 * 1024 * 11;
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
                hugePartitionDataLength,
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
        new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response =
        shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    // trigger NoBufferForHugePartitionException and get FAILED_REQUIRE_ID
    long requireId =
        shuffleServerClient.requirePreAllocation(
            appId, shuffleId, Lists.newArrayList(partitionId), hugePartitionDataLength, 3, 100);
    assertEquals(FAILED_REQUIRE_ID, requireId);

    shuffleId = 1;
    List<ShuffleBlockInfo> blockInfos2 =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                shuffleId,
                partitionId,
                0,
                hugePartitionDataLength,
                0,
                new byte[] {},
                Lists.newArrayList(),
                0,
                100,
                0));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks3 = Maps.newHashMap();
    partitionToBlocks3.put(partitionId, blockInfos2);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks2 = Maps.newHashMap();
    shuffleToBlocks2.put(shuffleId, partitionToBlocks3);

    sendShuffleDataRequest = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks2);
    response = shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
  }

  @Test
  public void sendDataWithoutRegisterTest() {
    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(0, 0, 0, 100, 0, new byte[] {}, Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest("sendDataWithoutRegisterTest", 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    // NO_REGISTER
    assertSame(StatusCode.INTERNAL_ERROR, response.getStatusCode());
  }

  @Test
  public void sendDataWithoutRequirePreAllocation() {
    String appId = "sendDataWithoutRequirePreAllocation";
    RssRegisterShuffleRequest registerShuffleRequest =
        new RssRegisterShuffleRequest(appId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    RssRegisterShuffleResponse registerResponse =
        shuffleServerClient.registerShuffle(registerShuffleRequest);
    assertSame(StatusCode.SUCCESS, registerResponse.getStatusCode());

    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(0, 0, 0, 100, 0, new byte[] {}, Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb :
        shuffleToBlocks.entrySet()) {
      List<RssProtos.ShuffleData> shuffleData = Lists.newArrayList();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<RssProtos.ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlocks.add(
              RssProtos.ShuffleBlock.newBuilder()
                  .setBlockId(sbi.getBlockId())
                  .setCrc(sbi.getCrc())
                  .setLength(sbi.getLength())
                  .setTaskAttemptId(sbi.getTaskAttemptId())
                  .setUncompressLength(sbi.getUncompressLength())
                  .setData(UnsafeByteOperations.unsafeWrap(sbi.getData().nioBuffer()))
                  .build());
        }
        shuffleData.add(
            RssProtos.ShuffleData.newBuilder()
                .setPartitionId(ptb.getKey())
                .addAllBlock(shuffleBlocks)
                .build());
      }

      RssProtos.SendShuffleDataRequest rpcRequest =
          RssProtos.SendShuffleDataRequest.newBuilder()
              .setAppId(appId)
              .setShuffleId(0)
              .setRequireBufferId(10000)
              .addAllShuffleData(shuffleData)
              .build();
      RssProtos.SendShuffleDataResponse response =
          shuffleServerClient.getBlockingStub().sendShuffleData(rpcRequest);
      assertEquals(RssProtos.StatusCode.NO_BUFFER, response.getStatus());
      assertTrue(
          response
              .getRetMsg()
              .contains("No such buffer ticket id, it may be discarded due to timeout"));
    }
  }

  @Test
  public void multipleShuffleResultTest() throws Exception {
    Set<Long> expectedBlockIds = Sets.newConcurrentHashSet();
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            "multipleShuffleResultTest", 100, Lists.newArrayList(new PartitionRange(0, 1)), "");
    shuffleServerClient.registerShuffle(rrsr);

    Runnable r1 =
        () -> {
          for (int i = 0; i < 100; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = ClientUtils.getBlockId(1L, 0L, (long) i);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 0, ptbs, 1);
            shuffleServerClient.reportShuffleResult(req1);
          }
        };
    Runnable r2 =
        () -> {
          for (int i = 100; i < 200; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = ClientUtils.getBlockId(1L, 1L, (long) i);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 1, ptbs, 1);
            shuffleServerClient.reportShuffleResult(req1);
          }
        };
    Runnable r3 =
        () -> {
          for (int i = 200; i < 300; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = ClientUtils.getBlockId(1L, 2L, (long) i);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 2, ptbs, 1);
            shuffleServerClient.reportShuffleResult(req1);
          }
        };
    Thread t1 = new Thread(r1);
    Thread t2 = new Thread(r2);
    Thread t3 = new Thread(r3);
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : expectedBlockIds) {
      blockIdBitmap.addLong(blockId);
    }

    RssGetShuffleResultRequest req =
        new RssGetShuffleResultRequest("multipleShuffleResultTest", 1, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap actualBlockIdBitmap = result.getBlockIdBitmap();
    assertEquals(blockIdBitmap, actualBlockIdBitmap);
  }

  private List<Long> getBlockIdList(int partitionId, int blockNum) {
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      blockIds.add(
          ClientUtils.getBlockId((long) partitionId, 0L, (long) atomicInteger.getAndIncrement()));
    }
    return blockIds;
  }

  private void addExpectedBlockIds(Roaring64NavigableMap bitmap, List<Long> blockIds) {
    for (Long blockId : blockIds) {
      bitmap.addLong(blockId);
    }
  }
}
