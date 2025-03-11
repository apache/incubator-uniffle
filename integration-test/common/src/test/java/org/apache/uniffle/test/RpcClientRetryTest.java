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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.MockedShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RpcClientRetryTest extends ShuffleReadWriteBase {
  private static List<ShuffleServerInfo> grpcShuffleServerInfoList;
  private static MockedShuffleWriteClientImpl shuffleWriteClientImpl;

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder(StorageType storageType) {
    return ShuffleClientFactory.newReadBuilder()
        .clientType(ClientType.GRPC)
        .storageType(storageType.name())
        .shuffleId(0)
        .partitionId(0)
        .indexReadLimit(100)
        .partitionNumPerRange(1)
        .partitionNum(10)
        .readBufferSize(1000);
  }

  public static MockedShuffleServer createMockedShuffleServer(
      int id, File tmpDir, ServerType serverType) throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    File dataDir1 = new File(tmpDir, id + "_1");
    File dataDir2 = new File(tmpDir, id + "_2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 5.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 15.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 600L);
    shuffleServerConf.set(ShuffleServerConf.SINGLE_BUFFER_FLUSH_BLOCKS_NUM_THRESHOLD, 1);
    shuffleServerConf.set(ShuffleServerConf.RPC_SERVER_PORT, 0);
    return new MockedShuffleServer(shuffleServerConf);
  }

  @BeforeEach
  public void initCluster(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    for (int i = 0; i < 3; i++) {
      grpcShuffleServers.add(createMockedShuffleServer(i, tmpDir, ServerType.GRPC));
    }

    startServers();
    grpcShuffleServerInfoList = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      grpcShuffleServerInfoList.add(
          new ShuffleServerInfo(
              String.format("127.0.0.1-%s", grpcShuffleServers.get(i).getGrpcPort()),
              grpcShuffleServers.get(i).getIp(),
              grpcShuffleServers.get(i).getGrpcPort()));
    }
  }

  @Test
  public void testCancelGrpc() throws InterruptedException {
    String testAppId = "testCancelGrpc";
    registerShuffleServer(testAppId, 1, 1, 1, false, 3000);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            2,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(grpcShuffleServerInfoList.get(0)));
    AtomicBoolean isCancel = new AtomicBoolean(false);
    Supplier<Boolean> needCancelRequest = () -> isCancel.get();
    SendShuffleDataResult result =
        shuffleWriteClientImpl.sendShuffleData(testAppId, blocks, needCancelRequest);
    assertEquals(2, result.getSuccessBlockIds().size());

    enableFirstNSendDataRequestsToFail(2);
    CompletableFuture<SendShuffleDataResult> future =
        CompletableFuture.supplyAsync(
            () -> shuffleWriteClientImpl.sendShuffleData(testAppId, blocks, needCancelRequest));
    // this ensure isCancel takes effect in rpc retry
    TimeUnit.SECONDS.sleep(1);
    isCancel.set(true);
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> future.isDone() && future.get().getSuccessBlockIds().size() == 0);
  }

  @AfterEach
  public void cleanEnv() throws Exception {
    if (shuffleWriteClientImpl != null) {
      shuffleWriteClientImpl.close();
    }
    grpcShuffleServerInfoList.clear();
    shutdownServers();
  }

  private static Stream<Arguments> testRpcRetryLogicProvider() {
    return Stream.of(
        Arguments.of(StorageType.MEMORY_LOCALFILE),
        // According to SERVER_BUFFER_CAPACITY & SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE,
        // data will be flushed to disk, so read from disk only
        Arguments.of(StorageType.LOCALFILE));
  }

  @ParameterizedTest
  @MethodSource("testRpcRetryLogicProvider")
  public void testRpcRetryLogic(StorageType storageType) {
    String testAppId = "testRpcRetryLogic";
    registerShuffleServer(testAppId, 3, 2, 2, true, 1000);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0, 0, 0, 3, 25, blockIdBitmap, expectedData, grpcShuffleServerInfoList);

    SendShuffleDataResult result =
        shuffleWriteClientImpl.sendShuffleData(testAppId, blocks, () -> false);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap successfulBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      successfulBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());
    assertEquals(blockIdBitmap, successfulBlockIdBitmap);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    ShuffleReadClientImpl readClient1 =
        baseReadBuilder(storageType)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(grpcShuffleServerInfoList)
            .retryMax(3)
            .retryIntervalMax(1)
            .build();

    // The data cannot be read because the maximum number of retries is 3
    enableFirstNReadRequestsToFail(4);
    try {
      validateResult(readClient1, expectedData);
      fail();
    } catch (Exception e) {
      // do nothing
    }
    disableFirstNReadRequestsToFail();

    ShuffleReadClientImpl readClient2 =
        baseReadBuilder(storageType)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(grpcShuffleServerInfoList)
            .retryMax(3)
            .retryIntervalMax(1)
            .build();

    // The data can be read because the reader will retry
    enableFirstNReadRequestsToFail(1);
    validateResult(readClient2, expectedData);
    disableFirstNReadRequestsToFail();
  }

  private static void enableFirstNReadRequestsToFail(int failedCount) {
    grpcShuffleServers.stream()
        .forEach(
            server ->
                ((MockedGrpcServer) server.getServer())
                    .getService()
                    .enableFirstNReadRequestToFail(failedCount));
  }

  private static void enableFirstNSendDataRequestsToFail(int failedCount) {
    grpcShuffleServers.stream()
        .forEach(
            server ->
                ((MockedGrpcServer) server.getServer())
                    .getService()
                    .enableFirstNSendDataRequestToFail(failedCount));
  }

  private static void disableFirstNReadRequestsToFail() {
    grpcShuffleServers.stream()
        .forEach(
            server ->
                ((MockedGrpcServer) server.getServer())
                    .getService()
                    .resetFirstNReadRequestToFail());
  }

  static class MockedShuffleWriteClientImpl extends ShuffleWriteClientImpl {
    MockedShuffleWriteClientImpl(ShuffleClientFactory.WriteClientBuilder builder) {
      super(builder);
    }
  }

  private void registerShuffleServer(
      String testAppId,
      int replica,
      int replicaWrite,
      int replicaRead,
      boolean replicaSkip,
      long retryIntervalMs) {

    shuffleWriteClientImpl =
        new MockedShuffleWriteClientImpl(
            ShuffleClientFactory.newWriteBuilder()
                .clientType(ClientType.GRPC.name())
                .retryMax(3)
                .retryIntervalMax(retryIntervalMs)
                .heartBeatThreadNum(1)
                .replica(replica)
                .replicaWrite(replicaWrite)
                .replicaRead(replicaRead)
                .replicaSkipEnabled(replicaSkip)
                .dataTransferPoolSize(1)
                .dataCommitPoolSize(1)
                .unregisterThreadPoolSize(10)
                .unregisterTimeSec(10)
                .unregisterRequestTimeSec(10));

    for (int i = 0; i < replica; i++) {
      shuffleWriteClientImpl.registerShuffle(
          grpcShuffleServerInfoList.get(i),
          testAppId,
          0,
          Lists.newArrayList(new PartitionRange(0, 0)),
          new RemoteStorageInfo(""),
          ShuffleDataDistributionType.NORMAL,
          1);
    }
  }
}
