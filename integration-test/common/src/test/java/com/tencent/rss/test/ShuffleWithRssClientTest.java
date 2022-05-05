/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.test;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShuffleWithRssClientTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    createShuffleServer(shuffleServerConf);
    startServers();
    shuffleServerInfo1 =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT);
    shuffleServerInfo2 =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(1).getIp(), SHUFFLE_SERVER_PORT + 1);
  }

  @Before
  public void createClient() {
    shuffleWriteClientImpl = new ShuffleWriteClientImpl(ClientType.GRPC.name(), 3, 1000, 1,
      1, 1, 1);
  }

  @After
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @Test
  public void rpcFailTest() throws Exception {
    String testAppId = "rpcFailTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // simulator a failed server
    ShuffleServerInfo fakeShuffleServerInfo =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT + 100);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo1, fakeShuffleServerInfo));
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    // There will no failed blocks when replica=2
    assertEquals(failedBlockIdBitmap.getLongCardinality(), 0);
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    boolean commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(
        shuffleServerInfo1, fakeShuffleServerInfo), testAppId, 0, 2);
    assertFalse(commitResult);

    // Report will success when replica=2
    Map<Integer, List<Long>> ptb = Maps.newHashMap();
    ptb.put(0, Lists.newArrayList(blockIdBitmap.stream().iterator()));
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    partitionToServers.put(0, Lists.newArrayList(
        shuffleServerInfo1, fakeShuffleServerInfo));
    shuffleWriteClientImpl.reportShuffleResult(partitionToServers, testAppId, 0, 0, ptb, 2);
    Roaring64NavigableMap report = shuffleWriteClientImpl.getShuffleResult("GRPC",
      Sets.newHashSet(shuffleServerInfo1, fakeShuffleServerInfo),
      testAppId, 0, 0);
    assertEquals(blockIdBitmap, report);
  }

  @Test
  public void reportMultipleServerTest() throws Exception {
    String testAppId = "reportMultipleServerTest";

    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 1, Lists.newArrayList(new PartitionRange(1, 1)), "");

    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo2,
        testAppId, 1, Lists.newArrayList(new PartitionRange(2, 2)), "");

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    partitionToServers.putIfAbsent(1, Lists.newArrayList(shuffleServerInfo1));
    partitionToServers.putIfAbsent(2, Lists.newArrayList(shuffleServerInfo2));
    Map<Integer, List<Long>> partitionToBlocks = Maps.newHashMap();
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < 5; i++ ) {
      blockIds.add(ClientUtils.getBlockId(1, 0, i));
    }
    partitionToBlocks.put(1, blockIds);
    blockIds = Lists.newArrayList();
    for (int i = 0; i < 7; i++ ) {
      blockIds.add(ClientUtils.getBlockId(2, 0, i));
    }
    partitionToBlocks.put(2, blockIds);
    shuffleWriteClientImpl
        .reportShuffleResult(partitionToServers, testAppId, 1, 0, partitionToBlocks, 1);

    Roaring64NavigableMap bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId,
        1, 0);
    assertTrue(bitmap.isEmpty());

    bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId,
        1, 1);
    assertEquals(5, bitmap.getLongCardinality());
    for (int i = 0; i < 5; i++) {
      assertTrue(bitmap.contains(partitionToBlocks.get(1).get(i)));
    }

    bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo1), testAppId,
        1, 2);
    assertTrue(bitmap.isEmpty());

    bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId,
        1, 0);
    assertTrue(bitmap.isEmpty());

    bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId,
        1, 1);
    assertTrue(bitmap.isEmpty());

    bitmap = shuffleWriteClientImpl
        .getShuffleResult("GRPC", Sets.newHashSet(shuffleServerInfo2), testAppId,
        1, 2);
    assertEquals(7, bitmap.getLongCardinality());
    for (int i = 0; i < 7; i++) {
      assertTrue(bitmap.contains(partitionToBlocks.get(2).get(i)));
    }
  }

  @Test
  public void writeReadTest() throws Exception {
    String testAppId = "writeReadTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo2,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    // send 1st commit, finish commit won't be sent to Shuffle server and data won't be persisted to disk
    boolean commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap,
        Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2), null);

    try {
      readClient.readShuffleBlockData();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to read all replicas for"));
    }
    readClient.close();

    // send 2nd commit, data will be persisted to disk
    commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);
    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap,
        Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2), null);
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    // commit will be failed because of fakeIp
    commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(new ShuffleServerInfo(
        "127.0.0.1-20001", "fakeIp", SHUFFLE_SERVER_PORT)), testAppId, 0, 2);
    assertFalse(commitResult);

    // wait resource to be deleted
    Thread.sleep(6000);

    // commit is ok, but finish shuffle rpc will failed because resource was deleted
    commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertFalse(commitResult);
  }

  @Test
  public void emptyTaskTest() {
    String testAppId = "emptyTaskTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    boolean commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1), testAppId, 0, 2);
    assertTrue(commitResult);
    commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo2), testAppId, 0, 2);
    assertFalse(commitResult);
  }
}
