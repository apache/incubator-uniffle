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

package com.tencent.rss.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.util.DefaultIdHelper;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MultiStorageFaultToleranceTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient shuffleServerClient;
  private static String REMOTE_STORAGE = HDFS_URI + "rss/multi_storage_fault";

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    String basePath = generateBasePath();
    shuffleServerConf.setDouble(ShuffleServerConf.CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setDouble(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setBoolean(ShuffleServerConf.UPLOADER_ENABLE, true);
    shuffleServerConf.setLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, REMOTE_STORAGE);
    shuffleServerConf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    shuffleServerConf.setLong(ShuffleServerConf.SHUFFLE_EXPIRED_TIMEOUT_MS, 5000L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 60L * 1000L * 60L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 20L * 1000L);
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE_HDFS_2.name());
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
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
  public void hdfsFaultTolerance() {
    try {
      String appId = "app_hdfs_fault_tolerance_data";
      Map<Long, byte[]> expectedData = Maps.newHashMap();
      Map<Integer, List<Integer>> map = Maps.newHashMap();
      map.put(2, Lists.newArrayList(0, 3));
      map.put(3, Lists.newArrayList(3));
      registerShuffle(appId, map);

      Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();

      List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
          2, 0, 1,11, 10 * 1024 * 1024, blockIdBitmap1, expectedData);

      List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
          3, 3, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

      List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
          2, 3, 2,9, 10 * 1024 * 1024, blockIdBitmap3, expectedData);

      assertEquals(0, ShuffleStorageUtils.getStorageIndex(2, appId, 2, 0));
      assertEquals(0, ShuffleStorageUtils.getStorageIndex(2, appId, 3, 3));
      assertEquals(0, ShuffleStorageUtils.getStorageIndex(2, appId, 2, 3));
      assertEquals(1, cluster.getDataNodes().size());
      cluster.stopDataNode(0);
      assertEquals(0, cluster.getDataNodes().size());

      sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks1);
      boolean isException = false;
      try {
        sendSinglePartitionToShuffleServer(appId, 3, 3,2, blocks2);
      } catch (RuntimeException re) {
        isException = true;
        assertTrue(re.getMessage().contains("Fail to finish"));
      }
      assertTrue(isException);

      cluster.startDataNodes(conf, 1, true, HdfsServerConstants.StartupOption.REGULAR,
          null, null, null, false, true);
      assertEquals(1, cluster.getDataNodes().size());

      sendSinglePartitionToShuffleServer(appId, 2, 3, 2, blocks3);

      validateResult(appId, 2, 0, blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), expectedData);
      validateResult(appId, 2, 3, blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), expectedData);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void hdfsFallbackTest() throws Exception {
    String appId = "fallback_test";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Map<Integer, List<Integer>> map = Maps.newHashMap();
    map.put(0, Lists.newArrayList(0));
    registerShuffle(appId, map);
    Roaring64NavigableMap blockBitmap = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 40, 10 * 1024 * 1024, blockBitmap, expectedData);
    assertEquals(1, cluster.getDataNodes().size());
    cluster.stopDataNode(0);
    assertEquals(0, cluster.getDataNodes().size());
    sendSinglePartitionToShuffleServer(appId, 0, 0, 0, blocks);
    validateResult(appId, 0, 0, blockBitmap, Roaring64NavigableMap.bitmapOf(0), expectedData);
    cluster.startDataNodes(conf, 1, true, HdfsServerConstants.StartupOption.REGULAR,
        null, null, null, false, true);
    assertEquals(1, cluster.getDataNodes().size());
  }

  private void registerShuffle(String appId, Map<Integer, List<Integer>> registerMap) {
    for (Map.Entry<Integer, List<Integer>> entry : registerMap.entrySet()) {
      for (int partition : entry.getValue()) {
        RssRegisterShuffleRequest rr = new RssRegisterShuffleRequest(appId, entry.getKey(),
            Lists.newArrayList(new PartitionRange(partition, partition)), REMOTE_STORAGE);
        shuffleServerClient.registerShuffle(rr);
      }
    }
  }

  @Test
  public void diskFaultTolerance() {
    String appId = "app_disk_fault_tolerance_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();

    Map<Integer, List<Integer>> map = Maps.newHashMap();
    map.put(2, Lists.newArrayList(1, 3));
    map.put(3, Lists.newArrayList(1));
    registerShuffle(appId, map);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        2, 1, 1,11, 10 * 1024 * 1024, blockIdBitmap1, expectedData);

    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        3, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        2, 3, 2,9, 10 * 1024 * 1024, blockIdBitmap3, expectedData);

    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        2, 1, 1, 11, 10 * 1024 * 1024, blockIdBitmap4, expectedData);

    assertEquals(1, ShuffleStorageUtils.getStorageIndex(2, appId, 2, 1));
    assertEquals(1, ShuffleStorageUtils.getStorageIndex(2, appId, 3, 1));
    assertEquals(1, ShuffleStorageUtils.getStorageIndex(2, appId, 2, 3));
    assertEquals(1, ShuffleStorageUtils.getStorageIndex(2, appId, 2, 1));
    try {
      sendSinglePartitionToShuffleServer(appId, 2, 1, 1, blocks1);
      sendSinglePartitionToShuffleServer(appId, 3, 1,2, blocks2);
      sendSinglePartitionToShuffleServer(appId, 2, 3, 2, blocks3);
      sendSinglePartitionToShuffleServer(appId, 2, 1, 1, blocks4);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    validateResult(appId, 2, 1, blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), expectedData);
    validateResult(appId, 3, 1, blockIdBitmap2, Roaring64NavigableMap.bitmapOf(2), expectedData);
    validateResult(appId, 2, 3, blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), expectedData);
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

  protected void validateResult(String appId, int shuffleId, int partitionId, Roaring64NavigableMap blockBitmap,
                                Roaring64NavigableMap taskBitmap, Map<Long, byte[]> expectedData) {
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE_HDFS_2.name(),
        appId, shuffleId, partitionId, 100, 1, 10, 1000, REMOTE_STORAGE, blockBitmap, taskBitmap,
        Lists.newArrayList(new ShuffleServerInfo("test", LOCALHOST, SHUFFLE_SERVER_PORT)), conf, new DefaultIdHelper());
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
    assertTrue(blockBitmap.equals(matched));
  }


  private Set<Long> getExpectBlockIds(List<ShuffleBlockInfo> blocks) {
    List<Long> expectBlockIds = Lists.newArrayList();
    blocks.forEach(b -> expectBlockIds.add(b.getBlockId()));
    return Sets.newHashSet(expectBlockIds);
  }
}
