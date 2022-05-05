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
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SparkClientWithLocalTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static File DATA_DIR1;
  private static File DATA_DIR2;
  private ShuffleServerGrpcClient shuffleServerClient;
  private List<ShuffleServerInfo> shuffleServerInfo =
      Lists.newArrayList(new ShuffleServerInfo("127.0.0.1-20001", LOCALHOST, SHUFFLE_SERVER_PORT));

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    DATA_DIR1 = new File(tmpDir, "data1");
    DATA_DIR2 = new File(tmpDir, "data2");
    String basePath = DATA_DIR1.getAbsolutePath() + "," + DATA_DIR2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void readTest1() {
    String testAppId = "localReadTest1";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    createTestData(testAppId, expectedData, blockIdBitmap, taskIdBitmap);
    blockIdBitmap.addLong((1 << Constants.TASK_ATTEMPT_ID_MAX_LENGTH));
    ShuffleReadClientImpl readClient;
    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap, shuffleServerInfo, null);
    validateResult(readClient, expectedData);
    try {
      // can't find all expected block id, data loss
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Blocks read inconsistent:"));
    } finally {
      readClient.close();
    }
  }


  @Test
  public void readTest2() {
    String testAppId = "localReadTest2";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);
    blocks = createShuffleBlockList(
        0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 0, 100, 1, 10, 1000,
        "", blockIdBitmap, taskIdBitmap, shuffleServerInfo, null);

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest3() throws Exception {
    String testAppId = "localReadTest3";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 0, 100, 1, 10, 1000,
        "", blockIdBitmap, taskIdBitmap, shuffleServerInfo, null);
    FileUtils.deleteDirectory(new File(DATA_DIR1.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
    FileUtils.deleteDirectory(new File(DATA_DIR2.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
    // sleep to wait delete operation
    Thread.sleep(2000);

    try {
      readClient.readShuffleBlockData();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to read all replicas for"));
    }
    readClient.close();
  }

  @Test
  public void readTest4() {
    String testAppId = "localReadTest4";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 1)));

    Map<Long, byte[]> expectedData1 = Maps.newHashMap();
    Map<Long, byte[]> expectedData2 = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 10, 30, blockIdBitmap1, expectedData1, mockSSI);
    sendTestData(testAppId, blocks);

    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    blocks = createShuffleBlockList(
        0, 1, 0, 10, 30, blockIdBitmap2, expectedData2, mockSSI);
    sendTestData(testAppId, blocks);

    blocks = createShuffleBlockList(
        0, 0, 0, 10, 30, blockIdBitmap1, expectedData1, mockSSI);
    sendTestData(testAppId, blocks);

    ShuffleReadClientImpl readClient1 = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 0, 100, 2, 10, 100,
        "", blockIdBitmap1, taskIdBitmap, shuffleServerInfo, null);
    ShuffleReadClientImpl readClient2 = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 1, 100, 2, 10, 100,
        "", blockIdBitmap2, taskIdBitmap, shuffleServerInfo, null);
    validateResult(readClient1, expectedData1);
    readClient1.checkProcessedBlockIds();
    readClient1.close();

    validateResult(readClient2, expectedData2);
    readClient2.checkProcessedBlockIds();
    readClient2.close();
  }

  @Test
  public void readTest5() {
    String testAppId = "localReadTest5";
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 1, 100, 2, 10, 1000,
        "", Roaring64NavigableMap.bitmapOf(), Roaring64NavigableMap.bitmapOf(),
        shuffleServerInfo, null);
    assertNull(readClient.readShuffleBlockData());
    readClient.checkProcessedBlockIds();
  }

  @Test
  public void readTest6() {
    String testAppId = "localReadTest6";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    Roaring64NavigableMap wrongBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    LongIterator iter = blockIdBitmap.getLongIterator();
    while (iter.hasNext()) {
      wrongBlockIdBitmap.addLong(iter.next() + (1 << Constants.TASK_ATTEMPT_ID_MAX_LENGTH));
    }

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        testAppId, 0, 0, 100, 1, 10, 100,
        "", wrongBlockIdBitmap, taskIdBitmap, shuffleServerInfo, null);
    assertNull(readClient.readShuffleBlockData());
    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Blocks read inconsistent:"));
    }
  }

  @Test
  public void readTest7() {
    String testAppId = "localReadTest7";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);

    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    blocks = createShuffleBlockList(
        0, 0, 1, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    blocks = createShuffleBlockList(
        0, 0, 2, 5, 30, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap, shuffleServerInfo, null);

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest8() {
    String testAppId = "localReadTest8";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 3);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    // test case: data generated by speculation task without report result
    blocks = createShuffleBlockList(
        0, 0, 1, 5, 30, Roaring64NavigableMap.bitmapOf(), Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);
    // test case: data generated by speculation task with report result
    blocks = createShuffleBlockList(
        0, 0, 2, 5, 30, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);

    blocks = createShuffleBlockList(
        0, 0, 3, 5, 30, Roaring64NavigableMap.bitmapOf(), Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap, shuffleServerInfo, null);

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest9() throws Exception {
    String testAppId = "localReadTest9";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    List<ShuffleBlockInfo> blocks;
    ShuffleReadClientImpl readClient;

    createTestData(testAppId, expectedData, blockIdBitmap, taskIdBitmap);
    Roaring64NavigableMap beforeAdded = RssUtils.deserializeBitMap(RssUtils.serializeBitMap(blockIdBitmap));
    // write data by another task, read data again, the cache for index file should be updated
    blocks = createShuffleBlockList(
        0, 0, 1, 3, 25, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);
    // test with un-changed expected blockId
    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", beforeAdded, taskIdBitmap,
        shuffleServerInfo, null);
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    // test with changed expected blockId
    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap,
        shuffleServerInfo, null);
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest10() throws Exception {
    String testAppId = "localReadTest10";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap unexpectedBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);
    // send some expected data
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
      0, 0, 0, 2, 30, expectedBlockIds, expectedData, mockSSI);
    sendTestData(testAppId, blocks);
    // send some unexpected data
    blocks = createShuffleBlockList(
      0, 0, 0, 2, 30, unexpectedBlockIds,
        Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks);
    // send some expected data
    blocks = createShuffleBlockList(
      0, 0, 1, 2, 30, expectedBlockIds, expectedData, mockSSI);
    sendTestData(testAppId, blocks);
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
      testAppId, 0, 0, 100, 1, 10, 1000,
      "", expectedBlockIds, taskIdBitmap, shuffleServerInfo, null);

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  protected void registerApp(String testAppId, List<PartitionRange> partitionRanges) {
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, 0, partitionRanges, "");
    shuffleServerClient.registerShuffle(rrsr);
  }

  protected void sendTestData(String testAppId, List<ShuffleBlockInfo> blocks) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks);

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
        testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    RssSendCommitRequest rscr = new RssSendCommitRequest(testAppId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(testAppId, 0);
    shuffleServerClient.finishShuffle(rfsr);
  }

  private void createTestData(
      String testAppId,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap) {
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.LOCALFILE.name(),
        testAppId,
        0,
        0,
        100,
        1,
        10,
        1000,
        "",
        blockIdBitmap,
        taskIdBitmap,
        shuffleServerInfo,
        null);
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }
}
