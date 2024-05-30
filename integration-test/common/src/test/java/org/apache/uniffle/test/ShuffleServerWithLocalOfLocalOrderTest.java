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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.segment.LocalOrderSegmentSplitter;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.ShuffleDataDistributionType.LOCAL_ORDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** This class is to test the local_order shuffle-data distribution */
public class ShuffleServerWithLocalOfLocalOrderTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;
  private static ShuffleServerConf grpcShuffleServerConfig;
  private static ShuffleServerConf nettyShuffleServerConfig;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);

    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String grpcBasePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, grpcBasePath);
    createShuffleServer(grpcShuffleServerConf);

    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    String nettyBasePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, nettyBasePath);
    createShuffleServer(nettyShuffleServerConf);

    startServers();

    grpcShuffleServerConfig = grpcShuffleServerConf;
    nettyShuffleServerConfig = nettyShuffleServerConf;
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType, String basePath)
      throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString("rss.server.app.expired.withoutHeartbeat", "5000");
    return shuffleServerConf;
  }

  @BeforeEach
  public void createClient() throws Exception {
    grpcShuffleServerClient =
        new ShuffleServerGrpcClient(
            LOCALHOST, grpcShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    nettyShuffleServerClient =
        new ShuffleServerGrpcNettyClient(
            LOCALHOST,
            nettyShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT),
            nettyShuffleServerConfig.getInteger(ShuffleServerConf.NETTY_SERVER_PORT));
  }

  @AfterEach
  public void closeClient() {
    grpcShuffleServerClient.close();
    nettyShuffleServerClient.close();
  }

  public static Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> createTestDataWithMultiMapIdx(
      BlockIdSet[] bitmaps, Map<Long, byte[]> expectedData) {
    for (int i = 0; i < 4; i++) {
      bitmaps[i] = BlockIdSet.empty();
    }

    // key: mapIdx
    Map<Integer, List<ShuffleBlockInfo>> p0 = new HashMap<>();
    List<ShuffleBlockInfo> blocks1 =
        createShuffleBlockList(0, 0, 0, 3, 25, bitmaps[0], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks2 =
        createShuffleBlockList(0, 0, 1, 3, 25, bitmaps[0], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks3 =
        createShuffleBlockList(0, 0, 2, 3, 25, bitmaps[0], expectedData, mockSSI);
    p0.put(0, blocks1);
    p0.put(1, blocks2);
    p0.put(2, blocks3);

    final List<ShuffleBlockInfo> blocks4 =
        createShuffleBlockList(0, 1, 1, 5, 25, bitmaps[1], expectedData, mockSSI);
    final Map<Integer, List<ShuffleBlockInfo>> p1 = new HashMap<>();
    p1.put(1, blocks4);

    final List<ShuffleBlockInfo> blocks5 =
        createShuffleBlockList(0, 2, 2, 4, 25, bitmaps[2], expectedData, mockSSI);
    final Map<Integer, List<ShuffleBlockInfo>> p2 = new HashMap<>();
    p2.put(2, blocks5);

    final List<ShuffleBlockInfo> blocks6 =
        createShuffleBlockList(0, 3, 3, 1, 25, bitmaps[3], expectedData, mockSSI);
    final Map<Integer, List<ShuffleBlockInfo>> p3 = new HashMap<>();
    p1.put(3, blocks6);

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, p0);
    partitionToBlocks.put(1, p1);
    partitionToBlocks.put(2, p2);
    partitionToBlocks.put(3, p3);
    return partitionToBlocks;
  }

  private static Stream<Arguments> testWriteAndReadWithSpecifiedMapRangeProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("testWriteAndReadWithSpecifiedMapRangeProvider")
  private void testWriteAndReadWithSpecifiedMapRange(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String testAppId = "testWriteAndReadWithSpecifiedMapRange";

    for (int i = 0; i < 4; i++) {
      RssRegisterShuffleRequest rrsr =
          new RssRegisterShuffleRequest(
              testAppId,
              0,
              Lists.newArrayList(new PartitionRange(i, i)),
              new RemoteStorageInfo(""),
              "",
              LOCAL_ORDER);
      shuffleServerClient.registerShuffle(rrsr);
    }

    /** Write the data to shuffle-servers */
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    BlockIdSet[] bitMaps = new BlockIdSet[4];

    // Create the shuffle block with the mapIdx
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> partitionToBlocksWithMapIdx =
        createTestDataWithMultiMapIdx(bitMaps, expectedData);

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks =
        partitionToBlocksWithMapIdx.entrySet().stream()
            .map(
                x ->
                    Pair.of(
                        x.getKey(),
                        x.getValue().values().stream()
                            .flatMap(a -> a.stream())
                            .collect(Collectors.toList())))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    // Flush the data to file
    RssSendCommitRequest rscr = new RssSendCommitRequest(testAppId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(testAppId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    /** Read the single partition data by specified [startMapIdx, endMapIdx) */
    // case1: get the mapIdx range [0, 1) of partition0
    final Set<Long> expectedBlockIds1 =
        partitionToBlocksWithMapIdx.get(0).get(0).stream()
            .map(x -> x.getBlockId())
            .collect(Collectors.toSet());
    final Map<Long, byte[]> expectedData1 =
        expectedData.entrySet().stream()
            .filter(x -> expectedBlockIds1.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

    Roaring64NavigableMap taskIds = Roaring64NavigableMap.bitmapOf(0);
    ShuffleDataResult sdr =
        readShuffleData(
            shuffleServerClient,
            testAppId,
            0,
            0,
            1,
            10,
            1000,
            0,
            new LocalOrderSegmentSplitter(taskIds, 1000));
    validate(sdr, expectedBlockIds1, expectedData1, new HashSet<>(Arrays.asList(0L)));

    // case2: get the mapIdx range [0, 2) of partition0
    final Set<Long> expectedBlockIds2 =
        partitionToBlocksWithMapIdx.get(0).get(1).stream()
            .map(x -> x.getBlockId())
            .collect(Collectors.toSet());
    expectedBlockIds2.addAll(expectedBlockIds1);
    final Map<Long, byte[]> expectedData2 =
        expectedData.entrySet().stream()
            .filter(x -> expectedBlockIds2.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    taskIds = Roaring64NavigableMap.bitmapOf(0, 1);
    sdr =
        readShuffleData(
            shuffleServerClient,
            testAppId,
            0,
            0,
            1,
            10,
            1000,
            0,
            new LocalOrderSegmentSplitter(taskIds, 1000));
    validate(sdr, expectedBlockIds2, expectedData2, new HashSet<>(Arrays.asList(0L, 1L)));

    // case2: get the mapIdx range [1, 3) of partition0
    final Set<Long> expectedBlockIds3 =
        partitionToBlocksWithMapIdx.get(0).get(1).stream()
            .map(x -> x.getBlockId())
            .collect(Collectors.toSet());
    expectedBlockIds3.addAll(
        partitionToBlocksWithMapIdx.get(0).get(2).stream()
            .map(x -> x.getBlockId())
            .collect(Collectors.toSet()));
    expectedBlockIds2.addAll(expectedBlockIds1);
    final Map<Long, byte[]> expectedData3 =
        expectedData.entrySet().stream()
            .filter(x -> expectedBlockIds3.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    taskIds = Roaring64NavigableMap.bitmapOf(1, 2);
    sdr =
        readShuffleData(
            shuffleServerClient,
            testAppId,
            0,
            0,
            1,
            10,
            1000,
            0,
            new LocalOrderSegmentSplitter(taskIds, 1000));
    validate(sdr, expectedBlockIds3, expectedData3, new HashSet<>(Arrays.asList(1L, 2L)));

    // case3: get the mapIdx range [0, Integer.MAX_VALUE) of partition0, it should always return all
    // data
    final Set<Long> expectedBlockIds4 =
        partitionToBlocks.get(0).stream().map(x -> x.getBlockId()).collect(Collectors.toSet());
    final Map<Long, byte[]> expectedData4 =
        expectedData.entrySet().stream()
            .filter(x -> expectedBlockIds4.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    taskIds = Roaring64NavigableMap.bitmapOf();
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    for (long blockId : expectedBlockIds4) {
      taskIds.add(new DefaultIdHelper(layout).getTaskAttemptId(blockId));
    }
    sdr =
        readShuffleData(
            shuffleServerClient,
            testAppId,
            0,
            0,
            1,
            10,
            10000,
            0,
            new LocalOrderSegmentSplitter(taskIds, 100000));
    validate(sdr, expectedBlockIds4, expectedData4, new HashSet<>(Arrays.asList(0L, 1L, 2L)));
  }

  private void validate(
      ShuffleDataResult sdr,
      Set<Long> expectedBlockIds,
      Map<Long, byte[]> expectedData,
      Set<Long> expectedTaskAttemptIds) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matched = 0;
    for (BufferSegment bs : bufferSegments) {
      if (expectedBlockIds.contains(bs.getBlockId())) {
        byte[] data = new byte[bs.getLength()];
        System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
        assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
        assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
        assertTrue(expectedBlockIds.contains(bs.getBlockId()));
        assertTrue(expectedTaskAttemptIds.contains(bs.getTaskAttemptId()));
        matched++;
      } else {
        fail();
      }
    }
    assertEquals(expectedBlockIds.size(), matched);
  }
}
