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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleWithRssClientTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;
  private static int rpcPort1;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(ServerType.GRPC);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    rpcPort1 = shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT);
    createShuffleServer(shuffleServerConf);
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger(
        "rss.rpc.server.port", shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT) + 1);
    shuffleServerConf.setInteger(
        "rss.jetty.http.port", shuffleServerConf.getInteger(ShuffleServerConf.JETTY_HTTP_PORT) + 1);
    createShuffleServer(shuffleServerConf);
    startServers();
    shuffleServerInfo1 =
        new ShuffleServerInfo(
            String.format("127.0.0.1-%s", grpcShuffleServers.get(0).getGrpcPort()),
            grpcShuffleServers.get(0).getIp(),
            grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerInfo2 =
        new ShuffleServerInfo(
            String.format("127.0.0.1-%s", grpcShuffleServers.get(0).getGrpcPort() + 1),
            grpcShuffleServers.get(1).getIp(),
            grpcShuffleServers.get(0).getGrpcPort() + 1);
  }

  @BeforeEach
  public void createClient() {
    shuffleWriteClientImpl =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(1000)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterRequestTimeSec(10)
            .build();
  }

  @AfterEach
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @Test
  public void rpcFailTest() throws Exception {
    String testAppId = "rpcFailTest";
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        0,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    BlockIdSet blockIdBitmap = BlockIdSet.empty();

    // simulator a failed server
    ShuffleServerInfo fakeShuffleServerInfo =
        new ShuffleServerInfo("127.0.0.1-20001", grpcShuffleServers.get(0).getIp(), rpcPort1 + 100);
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(shuffleServerInfo1, fakeShuffleServerInfo));
    SendShuffleDataResult result =
        shuffleWriteClientImpl.sendShuffleData(testAppId, blocks, () -> false);
    BlockIdSet failedBlockIdBitmap = BlockIdSet.empty();
    BlockIdSet succBlockIdBitmap = BlockIdSet.empty();
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.add(blockId);
    }
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.add(blockId);
    }
    // There will no failed blocks when replica=2
    assertEquals(failedBlockIdBitmap.getLongCardinality(), 0);
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    boolean commitResult =
        shuffleWriteClientImpl.sendCommit(
            Sets.newHashSet(shuffleServerInfo1, fakeShuffleServerInfo), testAppId, 0, 2);
    assertFalse(commitResult);

    // Report will success when replica=2
    Map<Integer, Set<Long>> ptb = Maps.newHashMap();
    ptb.put(0, Sets.newHashSet(blockIdBitmap.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, ptb);
    serverToPartitionToBlockIds.put(fakeShuffleServerInfo, ptb);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0, 2);
    BlockIdSet report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1, fakeShuffleServerInfo), testAppId, 0, 0);
    assertEquals(blockIdBitmap, report);
  }

  @Test
  public void reportBlocksToShuffleServerIfNecessary() {
    String testAppId = "reportBlocksToShuffleServerIfNecessary_appId";
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        1,
        Lists.newArrayList(new PartitionRange(1, 1)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);

    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo2,
        testAppId,
        1,
        Lists.newArrayList(new PartitionRange(2, 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);

    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    Map<Integer, Set<Long>> partitionToBlocks = Maps.newHashMap();
    Set<Long> blockIds = Sets.newHashSet();
    int partitionIdx = 1;
    for (int i = 0; i < 5; i++) {
      blockIds.add(layout.getBlockId(i, partitionIdx, 0));
    }
    partitionToBlocks.put(partitionIdx, blockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlocks);
    // case1
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 1, 0, 1);
    BlockIdSet bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId, 1, 0);
    assertTrue(bitmap.isEmpty());

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId, 1, partitionIdx);
    assertEquals(5, bitmap.getLongCardinality());
    for (Long b : partitionToBlocks.get(1)) {
      assertTrue(bitmap.contains(b));
    }
  }

  @Test
  public void reportMultipleServerTest() throws Exception {
    String testAppId = "reportMultipleServerTest";
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        1,
        Lists.newArrayList(new PartitionRange(1, 1)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);

    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo2,
        testAppId,
        1,
        Lists.newArrayList(new PartitionRange(2, 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    partitionToServers.putIfAbsent(1, Lists.newArrayList(shuffleServerInfo1));
    partitionToServers.putIfAbsent(2, Lists.newArrayList(shuffleServerInfo2));
    Map<Integer, Set<Long>> partitionToBlocks1 = Maps.newHashMap();
    Set<Long> blockIds = Sets.newHashSet();
    for (int i = 0; i < 5; i++) {
      blockIds.add(layout.getBlockId(i, 1, 0));
    }
    partitionToBlocks1.put(1, blockIds);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlocks1);

    Map<Integer, Set<Long>> partitionToBlocks2 = Maps.newHashMap();
    blockIds = Sets.newHashSet();
    for (int i = 0; i < 7; i++) {
      blockIds.add(layout.getBlockId(i, 2, 0));
    }
    partitionToBlocks2.put(2, blockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlocks2);

    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 1, 0, 1);

    BlockIdSet bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId, 1, 0);
    assertTrue(bitmap.isEmpty());

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId, 1, 1);
    assertEquals(5, bitmap.getLongCardinality());
    for (Long b : partitionToBlocks1.get(1)) {
      assertTrue(bitmap.contains(b));
    }

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId, 1, 2);
    assertTrue(bitmap.isEmpty());

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId, 1, 0);
    assertTrue(bitmap.isEmpty());

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId, 1, 1);
    assertTrue(bitmap.isEmpty());

    bitmap =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId, 1, 2);
    assertEquals(7, bitmap.getLongCardinality());
    for (Long b : partitionToBlocks2.get(2)) {
      assertTrue(bitmap.contains(b));
    }
  }

  @Test
  public void writeReadTest() throws Exception {
    String testAppId = "writeReadTest";
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        0,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo2,
        testAppId,
        0,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    BlockIdSet blockIdBitmap = BlockIdSet.empty();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks, () -> false);
    // send 1st commit, finish commit won't be sent to Shuffle server and data won't be persisted to
    // disk
    boolean commitResult =
        shuffleWriteClientImpl.sendCommit(
            Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);

    ShuffleReadClientImpl readClient =
        ShuffleClientFactory.newReadBuilder()
            .clientType(ClientType.GRPC)
            .storageType(StorageType.LOCALFILE.name())
            .appId(testAppId)
            .shuffleId(0)
            .partitionId(0)
            .indexReadLimit(100)
            .partitionNumPerRange(1)
            .partitionNum(10)
            .readBufferSize(1000)
            .basePath("")
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2))
            .build();
    assertNull(readClient.readShuffleBlockData());
    readClient.close();

    // send 2nd commit, data will be persisted to disk
    commitResult =
        shuffleWriteClientImpl.sendCommit(
            Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);
    readClient =
        ShuffleClientFactory.newReadBuilder()
            .clientType(ClientType.GRPC)
            .storageType(StorageType.LOCALFILE.name())
            .appId(testAppId)
            .shuffleId(0)
            .partitionId(0)
            .indexReadLimit(100)
            .partitionNumPerRange(1)
            .partitionNum(10)
            .readBufferSize(1000)
            .basePath("")
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    // commit will be failed because of fakeIp
    commitResult =
        shuffleWriteClientImpl.sendCommit(
            Sets.newHashSet(new ShuffleServerInfo("127.0.0.1-20001", "fakeIp", rpcPort1)),
            testAppId,
            0,
            2);
    assertFalse(commitResult);

    // wait resource to be deleted
    Thread.sleep(6000);

    // commit is ok, but finish shuffle rpc will failed because resource was deleted
    commitResult =
        shuffleWriteClientImpl.sendCommit(
            Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertFalse(commitResult);
  }

  @Test
  public void emptyTaskTest() {
    String testAppId = "emptyTaskTest";
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        0,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    boolean commitResult =
        shuffleWriteClientImpl.sendCommit(Sets.newHashSet(shuffleServerInfo1), testAppId, 0, 2);
    assertTrue(commitResult);
    commitResult =
        shuffleWriteClientImpl.sendCommit(Sets.newHashSet(shuffleServerInfo2), testAppId, 0, 2);
    assertFalse(commitResult);
  }

  @Test
  public void testRetryAssgin() throws Throwable {
    int maxTryTime = grpcShuffleServers.size();
    AtomicInteger tryTime = new AtomicInteger();
    String appId = "app-1";
    RemoteStorageInfo remoteStorage = new RemoteStorageInfo("");
    ShuffleAssignmentsInfo response = null;
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(ServerType.GRPC);
    int heartbeatInterval = shuffleServerConf.getInteger("rss.server.heartbeat.interval", 1000);
    Thread.sleep(heartbeatInterval * 2);
    shuffleWriteClientImpl.registerCoordinators(COORDINATOR_QUORUM);
    response =
        RetryUtils.retry(
            () -> {
              int currentTryTime = tryTime.incrementAndGet();
              ShuffleAssignmentsInfo shuffleAssignments =
                  shuffleWriteClientImpl.getShuffleAssignments(
                      appId, 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 1, -1);

              Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
                  shuffleAssignments.getServerToPartitionRanges();

              serverToPartitionRanges
                  .entrySet()
                  .forEach(
                      entry -> {
                        if (currentTryTime < maxTryTime) {
                          grpcShuffleServers.forEach(
                              (ss) -> {
                                if (ss.getId().equals(entry.getKey().getId())) {
                                  try {
                                    ss.stopServer();
                                  } catch (Exception e) {
                                    e.printStackTrace();
                                  }
                                }
                              });
                        }
                        shuffleWriteClientImpl.registerShuffle(
                            entry.getKey(),
                            appId,
                            0,
                            entry.getValue(),
                            remoteStorage,
                            ShuffleDataDistributionType.NORMAL,
                            -1);
                      });
              return shuffleAssignments;
            },
            heartbeatInterval,
            maxTryTime);

    assertNotNull(response);
  }
}
