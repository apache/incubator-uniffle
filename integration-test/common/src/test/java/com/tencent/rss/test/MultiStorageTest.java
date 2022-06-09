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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.client.util.DefaultIdHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.common.LocalStorage;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.handler.impl.ComposedClientReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.handler.impl.LocalFileQuorumClientReadHandler;
import com.tencent.rss.storage.handler.impl.UploadedHdfsClientReadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MultiStorageTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient shuffleServerClient;
  private static String REMOTE_STORAGE = HDFS_URI + "rss/multi_storage";

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), REMOTE_STORAGE);
    addDynamicConf(coordinatorConf, dynamicConf);
    String basePath = generateBasePath();
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE_HDFS_2.name());
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
    shuffleServerConf.setString(ShuffleServerConf.UPLOADER_BASE_PATH,  REMOTE_STORAGE);
    shuffleServerConf.setDouble(ShuffleServerConf.CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setLong(ShuffleServerConf.CLEANUP_INTERVAL_MS, 1000);
    shuffleServerConf.setDouble(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setBoolean(ShuffleServerConf.UPLOADER_ENABLE, true);
    shuffleServerConf.setLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    shuffleServerConf.setLong(ShuffleServerConf.SHUFFLE_EXPIRED_TIMEOUT_MS, 4000L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 60L * 1000L * 60L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 20L * 1000L);
    shuffleServerConf.setLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC, 30);
    shuffleServerConf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 400L * 1024L * 1024L);
    createAndStartServers(shuffleServerConf, coordinatorConf);
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @AfterEach
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void readUploadedDataTest() {
    String appId = "ap_read_uploaded_data";
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(1, 1)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(2, 2)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr4 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(4, 4)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    shuffleServerClient.registerShuffle(rr2);
    shuffleServerClient.registerShuffle(rr3);
    shuffleServerClient.registerShuffle(rr4);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Set<Long> expectedBlock2 = Sets.newHashSet();

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 1,3, 25, blockIdBitmap1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 1, 1,5,1024 * 1024, blockIdBitmap2, expectedData);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        0, 2, 2,4, 25, blockIdBitmap3, expectedData);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        0, 4, 3,1, 1024 * 1024, blockIdBitmap4, expectedData);


    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);

    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    partitionToBlockIds.put(1, new ArrayList<>(expectedBlock2));
    RssReportShuffleResultRequest rrp1 = new RssReportShuffleResultRequest(
        appId, 0, 1L, partitionToBlockIds, 2);
    shuffleServerClient.reportShuffleResult(rrp1);

    LocalStorage item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 0));
    assertTrue(item.canWrite());
    assertEquals(3 * 25, item.getNotUploadedSize(appId + "/" + 0));
    item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 1));
    assertTrue(item.canWrite());
    assertEquals(5 * 1024 * 1024, item.getNotUploadedSize(appId + "/" + 0));

    sendSinglePartitionToShuffleServer(appId, 0,2, 2L, blocks3);
    sendSinglePartitionToShuffleServer(appId, 0, 4, 3L, blocks4);

    item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 2));
    assertTrue(item.canWrite());
    assertEquals(3 * 25 + 4 * 25, item.getNotUploadedSize(appId + "/" + 0));

    item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 4));
    assertTrue(item.canWrite());
    assertEquals(5 * 1024 * 1024 + 1024 * 1024, item.getNotUploadedSize(appId + "/" + 0));


    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 0, 1);
    shuffleServerClient.getShuffleResult(rg2);
    RssGetShuffleResultRequest rg3 = new RssGetShuffleResultRequest(appId, 0, 2);
    shuffleServerClient.getShuffleResult(rg3);
    RssGetShuffleResultRequest rg4 = new RssGetShuffleResultRequest(appId, 0, 4);
    shuffleServerClient.getShuffleResult(rg4);

    readShuffleData(shuffleServerClient, appId, 0, 0, 1, 10, 100, 0);
    readShuffleData(shuffleServerClient, appId, 0, 1, 1, 10, 100, 0);


    wait(appId);

    item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 0));
    assertTrue(item.canWrite());
    assertEquals(0, item.getNotUploadedSize(appId + "/" + 0));

    item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 1));
    assertTrue(item.canWrite());
    assertEquals(0, item.getNotUploadedSize(appId + "/" + 0));

    ShuffleDataResult result = readShuffleData(shuffleServerClient, appId, 0, 0,
        1, 10, 1000,  0);
    assertEquals(0, result.getData().length);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 0, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), Lists.newArrayList(), conf, new DefaultIdHelper());
    validateResult(readClient, expectedData, blockIdBitmap1);

    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 1, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap2, Roaring64NavigableMap.bitmapOf(1), Lists.newArrayList(), conf, new DefaultIdHelper());
    validateResult(readClient, expectedData, blockIdBitmap2);

    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 2, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), Lists.newArrayList(), conf, new DefaultIdHelper());
    validateResult(readClient, expectedData, blockIdBitmap3);

    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 4, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap4, Roaring64NavigableMap.bitmapOf(3), Lists.newArrayList(), conf, new DefaultIdHelper());
    validateResult(readClient, expectedData, blockIdBitmap4);
  }

  private void wait(String appId) {
    do {
      try {
        RssAppHeartBeatRequest ra = new RssAppHeartBeatRequest(appId, 1000);
        shuffleServerClient.sendHeartBeat(ra);
        boolean uploadFinished = true;
        for (int i = 0; i < 4; i++) {
          LocalStorage localStorage = (LocalStorage) shuffleServers.get(0).getStorageManager()
              .selectStorage(new ShuffleDataReadEvent(appId, 0, i));
          String path = ShuffleStorageUtils.getFullShuffleDataFolder(localStorage.getBasePath(),
              ShuffleStorageUtils.getShuffleDataPath(appId, 0, i, i));
          File file = new File(path);
          if (file.exists()) {
            uploadFinished = false;
            break;
          }
        }
        if (uploadFinished) {
          break;
        }
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    } while (true);
  }

  @Test
  public void readLocalDataTest() {
    String appId = "app_read_not_uploaded_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(1, 1)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(2, 2)), REMOTE_STORAGE);
    RssRegisterShuffleRequest rr4 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(3, 3)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    shuffleServerClient.registerShuffle(rr2);
    shuffleServerClient.registerShuffle(rr3);
    shuffleServerClient.registerShuffle(rr4);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        1, 0, 1,3, 25, blockIdBitmap1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        1, 1, 2,5,1024 * 1024, blockIdBitmap2, expectedData);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        1, 2, 3,4, 25, blockIdBitmap3, expectedData);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        1, 3, 4,1, 1024 * 1024, blockIdBitmap4, expectedData);

    sendSinglePartitionToShuffleServer(appId, 1,0, 1L, blocks1);
    sendSinglePartitionToShuffleServer(appId, 1,1, 2L, blocks2);
    sendSinglePartitionToShuffleServer(appId, 1,2, 3L, blocks3);
    sendSinglePartitionToShuffleServer(appId, 1,3, 4L, blocks4);

    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 1, 0);
    shuffleServerClient.getShuffleResult(rg1);
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 1, 1);
    shuffleServerClient.getShuffleResult(rg2);
    RssGetShuffleResultRequest rg3 = new RssGetShuffleResultRequest(appId, 1, 2);
    shuffleServerClient.getShuffleResult(rg3);
    RssGetShuffleResultRequest rg4 = new RssGetShuffleResultRequest(appId, 1, 3);
    shuffleServerClient.getShuffleResult(rg4);

    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 2, expectedData, getExpectBlockIds(blocks3));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 0, expectedData, getExpectBlockIds(blocks1));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 1, expectedData, getExpectBlockIds(blocks2));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 3, expectedData, getExpectBlockIds(blocks4));
    Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
    ShuffleDataResult sdr = readShuffleData(shuffleServerClient, appId, 1, 0,
        1, 10, 1000,  0);
    assertEquals(0, sdr.getData().length);
  }

  @Test
  public void readMixedDataTest() {
    String appId = "app_read_mix_data";
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 1,15, 1024 * 1024, blockIdBitmap1, expectedData);

    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);

    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    RssReportShuffleResultRequest rrp1 = new RssReportShuffleResultRequest(
        appId, 0, 1L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp1);

    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 0, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1),
        Lists.newArrayList(new ShuffleServerInfo("test", LOCALHOST, SHUFFLE_SERVER_PORT)), conf, new DefaultIdHelper());

    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    assertNotNull(csb);
    assertNotNull(csb.getByteBuffer());
    for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
      if (compareByte(entry.getValue(), csb.getByteBuffer())) {
        matched.addLong(entry.getKey());
      }
    }
    wait(appId);

    csb = readClient.readShuffleBlockData();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap1.equals(matched));

    ShuffleDataResult sdr = readShuffleData(shuffleServerClient, appId, 0, 0,
        1, 10, 1000, 0);
    assertEquals(0, sdr.getData().length);

    List<ShuffleBlockInfo> blocks5 = createShuffleBlockList(
        0, 0, 1,15, 1024 * 1024, blockIdBitmap1, expectedData);
    partitionToBlocks.clear();
    shuffleToBlocks.clear();
    partitionToBlocks.put(0, blocks5);
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs5 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    LocalStorage localStorage = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 0));
    String path = ShuffleStorageUtils.getFullShuffleDataFolder(localStorage.getBasePath(),
        ShuffleStorageUtils.getShuffleDataPath(appId, 0, 0, 0));
    File file = new File(path);
    assertFalse(file.exists());
    try {
      shuffleServerClient.sendShuffleData(rs5);
      shuffleServerClient.sendCommit(rc1);
      shuffleServerClient.finishShuffle(rf1);
      shuffleServerClient.reportShuffleResult(rrp1);
    } catch (Exception e) {
      fail();
    }
    assertFalse(file.exists());
  }

  @Test
  public void readDifferentStorageData() {
    String appId = "app_read_diff_data";
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 1, 40, 10 * 1024 * 1024, blockIdBitmap1, expectedData);
    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);
    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    RssReportShuffleResultRequest rrp1 = new RssReportShuffleResultRequest(
        appId, 0, 1L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp1);
    Set<Long> expectedBlock2 = Sets.newHashSet();
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 0, 2, 40, 1024, blockIdBitmap1, expectedData);
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));
    partitionToBlocks.put(0, blocks2);
    RssSendShuffleDataRequest rs2 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs2);
    shuffleServerClient.sendCommit(rc1);
    shuffleServerClient.finishShuffle(rf1);
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    RssReportShuffleResultRequest rrp2 = new RssReportShuffleResultRequest(
        appId, 0, 2L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp2);
    shuffleServerClient.finishShuffle(rf1);
    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 0, 0, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1, 2),
        Lists.newArrayList(new ShuffleServerInfo("test", LOCALHOST, SHUFFLE_SERVER_PORT)), conf, new DefaultIdHelper());

    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    assertNotNull(csb);
    assertNotNull(csb.getByteBuffer());
    for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
      if (compareByte(entry.getValue(), csb.getByteBuffer())) {
        matched.addLong(entry.getKey());
      }
    }
    wait(appId);

    csb = readClient.readShuffleBlockData();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap1.equals(matched));
  }

  @Test
  public void readDifferentStorageDataWithFilter() {
    String appId = "app_read_diff_data_filter";
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
      Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Set<Long> expectedBlock2 = Sets.newHashSet();
    Set<Long> actualBlock1 = Sets.newHashSet();
    Set<Long> actualBlock2 = Sets.newHashSet();

    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap processBitmap = Roaring64NavigableMap.bitmapOf();

    // send large blocks to HDFS
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
      0, 0, 1, 40, 10 * 1024 * 1024, blockIdBitmap, expectedData);
    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);
    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    RssReportShuffleResultRequest rrp1 = new RssReportShuffleResultRequest(
      appId, 0, 1L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp1);

    // send small blocks to local file
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
      0, 0, 2, 40, 1024, blockIdBitmap, expectedData);
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));
    partitionToBlocks.put(0, blocks2);
    RssSendShuffleDataRequest rs2 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs2);
    shuffleServerClient.sendCommit(rc1);
    shuffleServerClient.finishShuffle(rf1);
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    RssReportShuffleResultRequest rrp2 = new RssReportShuffleResultRequest(
      appId, 0, 2L, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp2);
    shuffleServerClient.finishShuffle(rf1);
    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);

    LocalFileQuorumClientReadHandler localFileQuorumClientReadHandler = new LocalFileQuorumClientReadHandler(
      appId, 0, 0, 0, 1, 3,
      20 * 1024 , blockIdBitmap, processBitmap, Lists.newArrayList(shuffleServerClient));
    HdfsClientReadHandler hdfsClientReadHandler = new HdfsClientReadHandler(
      appId, 0, 0, 0, 1, 3,
      100 * 1024 * 1024, blockIdBitmap, processBitmap, REMOTE_STORAGE, conf);
    UploadedHdfsClientReadHandler uploadedHdfsClientReadHandler = new UploadedHdfsClientReadHandler(
      appId, 0, 0, 0, 1, 3,
      20 * 1024, blockIdBitmap, processBitmap, REMOTE_STORAGE, conf);
    ClientReadHandler[] handlers = new ClientReadHandler[3];
    handlers[0] = localFileQuorumClientReadHandler;
    handlers[1] = hdfsClientReadHandler;
    handlers[2] = uploadedHdfsClientReadHandler;
    ComposedClientReadHandler handler = new ComposedClientReadHandler(handlers);

    // read the 1-th segment from localfile
    ShuffleDataResult result = null;
    result = handler.readShuffleData();
    result.getBufferSegments().forEach(b -> actualBlock1.add(b.getBlockId()));
    result.getBufferSegments().forEach(b -> processBitmap.add(b.getBlockId()));

    // flush the localfile to uploaded HDFS
    wait(appId);

    // read the 2-th segment from uploaded HDFS and other segments from HDFS
    while(result != null) {
      result = handler.readShuffleData();
      if (result != null) {
        result.getBufferSegments().forEach(b -> actualBlock2.add(b.getBlockId()));
        result.getBufferSegments().forEach(b -> processBitmap.add(b.getBlockId()));
      }
    }

    assertEquals(expectedBlock1.size() + expectedBlock2.size(),
      actualBlock1.size() + actualBlock2.size());

    expectedBlock2.forEach(b -> expectedBlock1.add(b));
    actualBlock2.forEach(b -> actualBlock1.add(b));
    assert (expectedBlock1.equals(actualBlock1));
  }

  protected void validateResult(
    Map<Long, byte[]> expectedData,
    ShuffleDataResult sdr) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    assertEquals(expectedData.size(), bufferSegments.size());
    for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
      BufferSegment bs = findBufferSegment(entry.getKey(), bufferSegments);
      assertNotNull(bs);
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
    }
  }

  private BufferSegment findBufferSegment(long blockId, List<BufferSegment> bufferSegments) {
    for (BufferSegment bs : bufferSegments) {
      if (bs.getBlockId() == blockId) {
        return bs;
      }
    }
    return null;
  }

  @Test
  public void diskUsageTest() {
    String appId = "app_read_diskusage_data";
    long originSize = shuffleServers.get(0).getShuffleBufferManager().getCapacity();
    Map<Long, byte[]> expectedData = Maps.newHashMap();

    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 2,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);

    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 3,
        Lists.newArrayList(new PartitionRange(1, 1)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr2);

    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 2,
        Lists.newArrayList(new PartitionRange(1, 1)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr3);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        2, 0, 1,30, 10 * 1024 * 1024, blockIdBitmap1, expectedData);

    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        3, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        2, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap3, expectedData);

    LocalStorage item = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 2, 0));
    item.createMetadataIfNotExist(appId + "/" + 2);
    item.getLock(appId + "/" + 2).readLock().lock();
    sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks1);
    assertFalse(item.canWrite());
    assertEquals(30 * 1024 * 1024 * 10, item.getNotUploadedSize(appId + "/" + 2));
    assertEquals(1, item.getNotUploadedPartitions(appId + "/" + 2).getCardinality());
    boolean isException = false;
    try {
      sendSinglePartitionToShuffleServer(appId, 2, 1, 2, blocks3);
    } catch (RuntimeException re) {
      isException = true;
      assertTrue(re.getMessage().contains("Can't finish shuffle process"));
    }
    item.getLock(appId + "/" + 2).readLock().unlock();
    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
    assertEquals(originSize, shuffleServers.get(0).getShuffleBufferManager().getCapacity());
    assertTrue(isException);
    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 2, 0);
    shuffleServerClient.getShuffleResult(rg1);
    validateResult(appId, 2, 0, expectedData, Sets.newHashSet());
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, 2, 0, 100, 1, 10, 1000, REMOTE_STORAGE,
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1),
        Lists.newArrayList(new ShuffleServerInfo("test", LOCALHOST, SHUFFLE_SERVER_PORT)), conf, new DefaultIdHelper());
    validateResult(readClient, expectedData, blockIdBitmap1);
    try {
      sendSinglePartitionToShuffleServer(appId, 3, 1,2, blocks2);
    } catch (RuntimeException re) {
      fail();
    }
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 3, 1);
    shuffleServerClient.getShuffleResult(rg2);
    validateResult(appId, 3, 1, expectedData,
        getExpectBlockIds(blocks2));

    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
  }

  @Test
  public void removeMetaTest() {
    String appId = "app_read_diskusage_data_without_report";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 2,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 3,
        Lists.newArrayList(new PartitionRange(1, 1)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr2);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        2, 0, 1,30, 10 * 1024 * 1024, blockIdBitmap1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        3, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

    sendSinglePartitionToShuffleServerWithoutReport(appId, 2, 2, 2, blocks1);
    sendSinglePartitionToShuffleServerWithoutReport(appId, 3, 1,2, blocks2);
    shuffleServers.get(0).getShuffleTaskManager().removeResources(appId);
    LocalStorage storage = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 2, 0));
    Uninterruptibles.sleepUninterruptibly(1500, TimeUnit.MILLISECONDS);
    Set<String> keys = storage.getShuffleMetaSet();
    assertTrue(keys.isEmpty());
    storage = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 3, 1));
    keys = storage.getShuffleMetaSet();
    assertTrue(keys.isEmpty());

    appId = "app_read_diskusage_data_with_report";
    rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)), REMOTE_STORAGE);
    shuffleServerClient.registerShuffle(rr1);
    blocks1 = createShuffleBlockList(
        0, 0, 1,30, 10 * 1024, blockIdBitmap1, expectedData);
    sendSinglePartitionToShuffleServer(appId, 0, 0, 2, blocks1);
    shuffleServers.get(0).getShuffleTaskManager().removeResources(appId);
    storage = (LocalStorage) shuffleServers.get(0).getStorageManager()
        .selectStorage(new ShuffleDataReadEvent(appId, 0, 0));
    Uninterruptibles.sleepUninterruptibly(1500, TimeUnit.MILLISECONDS);
    keys = storage.getShuffleMetaSet();
    assertTrue(keys.isEmpty());
  }

  private void sendSinglePartitionToShuffleServer(String appId, int shuffle, int partition,
        long taskAttemptId, List<ShuffleBlockInfo> blocks) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    Set<Long> expectBlockIds = getExpectBlockIds(blocks);
    partitionToBlocks.put(partition, blocks);
    shuffleToBlocks.put(shuffle, partitionToBlocks);
    RssSendShuffleDataRequest rs = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs);
    RssSendCommitRequest rc = new RssSendCommitRequest(appId, shuffle);
    shuffleServerClient.sendCommit(rc);
    RssFinishShuffleRequest rf = new RssFinishShuffleRequest(appId, shuffle);
    shuffleServerClient.finishShuffle(rf);
    partitionToBlockIds.put(shuffle, new ArrayList<>(expectBlockIds));
    RssReportShuffleResultRequest rrp = new RssReportShuffleResultRequest(
        appId, shuffle, taskAttemptId, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp);
  }

  private void sendSinglePartitionToShuffleServerWithoutReport(String appId, int shuffle, int partition,
      long taskAttemptId, List<ShuffleBlockInfo> blocks) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partition, blocks);
    shuffleToBlocks.put(shuffle, partitionToBlocks);
    RssSendShuffleDataRequest rs = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs);
    RssSendCommitRequest rc = new RssSendCommitRequest(appId, shuffle);
    shuffleServerClient.sendCommit(rc);
    RssFinishShuffleRequest rf = new RssFinishShuffleRequest(appId, shuffle);
    shuffleServerClient.finishShuffle(rf);
  }

  protected void validateResult(String appId, int shuffleId, int partitionId,
     Map<Long, byte[]> expectedData, Set<Long> expectBlockIds) {
    int matched = 0;
    int index = 0;
    ShuffleDataResult result = null;
    List<ShuffleDataSegment> shuffleDataSegments = readShuffleIndexSegments(
        shuffleServerClient, appId, shuffleId, partitionId, 1, 10, 1000);
    do {
      if (result != null) {
        byte[] buffer = result.getData();
        for (BufferSegment bs : result.getBufferSegments()) {
          if (expectBlockIds.contains(bs.getBlockId())) {
            byte[] data = new byte[bs.getLength()];
            System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
            assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
            assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
            matched++;
          }
          assertTrue(expectBlockIds.contains(bs.getBlockId()));
        }
      }
      result = readShuffleData(shuffleServerClient, appId, shuffleId, partitionId,
          1, 10, index, shuffleDataSegments);
      ++index;
    } while(result != null && result.getData() != null
      && result.getBufferSegments() != null && !result.getBufferSegments().isEmpty());
    assertEquals(expectBlockIds.size(), matched);
  }

  protected void validateResult(ShuffleReadClientImpl readClient, Map<Long, byte[]> expectedData,
                                Roaring64NavigableMap blockIdBitmap) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap.equals(matched));
  }

  private Set<Long> getExpectBlockIds(List<ShuffleBlockInfo> blocks) {
    List<Long> expectBlockIds = Lists.newArrayList();
    blocks.forEach(b -> expectBlockIds.add(b.getBlockId()));
    return Sets.newHashSet(expectBlockIds);
  }
}
