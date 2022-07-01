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

package com.tencent.rss.server;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.tencent.rss.common.RemoteStorageInfo;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.server.buffer.PreAllocatedBufferInfo;
import com.tencent.rss.server.buffer.ShuffleBuffer;
import com.tencent.rss.server.buffer.ShuffleBufferManager;
import com.tencent.rss.server.storage.StorageManager;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleTaskManagerTest extends HdfsTestBase {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  @AfterAll
  public static void tearDown() {
    ShuffleServerMetrics.clear();
  }

  @Test
  public void registerShuffleTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    ShuffleServer shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf,
        shuffleServer.getShuffleFlushManager(), shuffleServer.getShuffleBufferManager(), null);

    String appId = "registerTest1";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Lists.newArrayList(new PartitionRange(2, 3)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleServer.getShuffleBufferManager().getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void writeProcessTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    String remoteStorage = HDFS_URI + "rss/test";
    String appId = "testAppId";
    int shuffleId = 1;
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 3000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    ShuffleServer shuffleServer = new ShuffleServer(conf);
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    StorageManager storageManager = shuffleServer.getStorageManager();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(
        conf, shuffleFlushManager, shuffleBufferManager, storageManager);
    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Lists.newArrayList(new PartitionRange(1, 1)), new RemoteStorageInfo(remoteStorage));
    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Lists.newArrayList(new PartitionRange(2, 2)), new RemoteStorageInfo(remoteStorage));
    List<ShufflePartitionedBlock> expectedBlocks1 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks2 = Lists.newArrayList();
    Map<Long, PreAllocatedBufferInfo> bufferIds = shuffleTaskManager.getRequireBufferIds();

    shuffleTaskManager.requireBuffer(10);
    shuffleTaskManager.requireBuffer(10);
    shuffleTaskManager.requireBuffer(10);
    assertEquals(3, bufferIds.size());
    // required buffer should be clear if doesn't receive data after timeout
    Thread.sleep(6000);
    assertEquals(0, bufferIds.size());

    shuffleTaskManager.commitShuffle(appId, shuffleId);

    // won't flush for partition 1-1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData0.getBlockList()));
    long requireId = shuffleTaskManager.requireBuffer(35);
    assertEquals(1, bufferIds.size());
    PreAllocatedBufferInfo pabi = bufferIds.get(requireId);
    assertEquals(35, pabi.getRequireSize());
    StatusCode sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData0.getBlockList());
    // the required id won't be removed in shuffleTaskManager, it is removed in Grpc service
    assertEquals(1, bufferIds.size());
    assertEquals(StatusCode.SUCCESS, sc);
    shuffleTaskManager.commitShuffle(appId, shuffleId);
    assertEquals(1, shuffleFlushManager.getCommittedBlockIds(appId, shuffleId).getLongCardinality());

    // flush for partition 1-1
    ShufflePartitionedData partitionedData1 = createPartitionedData(1, 2, 35);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData1.getBlockList()));
    shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData1);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData1.getBlockList());
    assertEquals(StatusCode.SUCCESS, sc);
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1);

    // won't flush for partition 1-1
    ShufflePartitionedData partitionedData2 = createPartitionedData(1, 1, 30);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData2.getBlockList()));
    // receive un-preAllocation data
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData2);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData2.getBlockList());
    assertEquals(StatusCode.SUCCESS, sc);

    // won't flush for partition 2-2
    ShufflePartitionedData partitionedData3 = createPartitionedData(2, 1, 30);
    expectedBlocks2.addAll(Lists.newArrayList(partitionedData3.getBlockList()));
    shuffleTaskManager.requireBuffer(30);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData3);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData3.getBlockList());
    assertEquals(StatusCode.SUCCESS, sc);

    // flush for partition 2-2
    ShufflePartitionedData partitionedData4 = createPartitionedData(2, 1, 35);
    expectedBlocks2.addAll(Lists.newArrayList(partitionedData4.getBlockList()));
    shuffleTaskManager.requireBuffer(35);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData4);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData4.getBlockList());
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleTaskManager.commitShuffle(appId, shuffleId);
    // 3 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3);

    // flush for partition 1-1
    ShufflePartitionedData partitionedData5 = createPartitionedData(1, 2, 35);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData5.getBlockList());
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData5.getBlockList()));
    shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData5);
    assertEquals(StatusCode.SUCCESS, sc);

    // 2 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3 + 2);
    shuffleTaskManager.commitShuffle(appId, shuffleId);
    shuffleTaskManager.commitShuffle(appId, shuffleId);

    validate(appId, shuffleId, 1, expectedBlocks1, remoteStorage);
    validate(appId, shuffleId, 2, expectedBlocks2, remoteStorage);

    // flush for partition 0-1
    ShufflePartitionedData partitionedData7 = createPartitionedData(1, 2, 35);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData7.getBlockList());
    shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData7);
    assertEquals(StatusCode.SUCCESS, sc);

    // 2 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3 + 2 + 2);
    shuffleFlushManager.removeResources(appId);
    try {
      shuffleTaskManager.commitShuffle(appId, shuffleId);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Shuffle data commit timeout for"));
    }
  }

  @Test
  public void clearTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/clearTest";
    int shuffleId = 1;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, storageBasePath);
    conf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    ShuffleServer shuffleServer = new ShuffleServer(conf);
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    StorageManager storageManager = shuffleServer.getStorageManager();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf, shuffleFlushManager, shuffleBufferManager, storageManager);
    shuffleTaskManager.registerShuffle(
        "clearTest1", shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    shuffleTaskManager.registerShuffle(
        "clearTest2", shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    shuffleTaskManager.refreshAppId("clearTest1");
    shuffleTaskManager.refreshAppId("clearTest2");
    assertEquals(2, shuffleTaskManager.getAppIds().size());
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);

    // keep refresh status of application "clearTest1"
    int retry = 0;
    while (retry < 10) {
      Thread.sleep(1000);
      shuffleTaskManager.cacheShuffleData("clearTest1", shuffleId, false, partitionedData0);
      shuffleTaskManager.updateCachedBlockIds("clearTest1", shuffleId, partitionedData0.getBlockList());
      shuffleTaskManager.refreshAppId("clearTest1");
      shuffleTaskManager.checkResourceStatus();
      retry++;
    }
    // application "clearTest2" was removed according to rss.server.app.expired.withoutHeartbeat
    assertEquals(Sets.newHashSet("clearTest1"), shuffleTaskManager.getAppIds().keySet());
    assertEquals(1, shuffleTaskManager.getCachedBlockIds("clearTest1", shuffleId).getLongCardinality());

    // register again
    shuffleTaskManager.registerShuffle(
        "clearTest2", shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    shuffleTaskManager.refreshAppId("clearTest2");
    shuffleTaskManager.checkResourceStatus();
    assertEquals(Sets.newHashSet("clearTest1", "clearTest2"), shuffleTaskManager.getAppIds().keySet());
    Thread.sleep(5000);
    shuffleTaskManager.checkResourceStatus();
    // wait resource delete
    Thread.sleep(3000);
    assertEquals(Sets.newHashSet(), shuffleTaskManager.getAppIds().keySet());
    assertTrue(shuffleTaskManager.getCachedBlockIds("clearTest1", shuffleId).isEmpty());
  }

  @Test
  public void getBlockIdsByPartitionIdTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(
        conf, null, null, null);

    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    int expectedPartitionId = 5;
    Roaring64NavigableMap bitmapBlockIds = Roaring64NavigableMap.bitmapOf();
    for (int taskId = 1; taskId < 10; taskId++) {
      for (int partitionId = 1; partitionId < 10; partitionId++) {
        for (int i = 0; i < 2; i++) {
          long blockId = getBlockId(partitionId, taskId, i);
          bitmapBlockIds.addLong(blockId);
          if (partitionId == expectedPartitionId) {
            expectedBlockIds.addLong(blockId);
          }
        }
      }
    }
    Roaring64NavigableMap resultBlockIds = shuffleTaskManager.getBlockIdsByPartitionId(
        expectedPartitionId, bitmapBlockIds);
    assertEquals(expectedBlockIds, resultBlockIds);

    bitmapBlockIds.addLong(getBlockId(0, 0, 0));
    resultBlockIds = shuffleTaskManager.getBlockIdsByPartitionId(0, bitmapBlockIds);
    assertEquals(Roaring64NavigableMap.bitmapOf(0L), resultBlockIds);

    long expectedBlockId = getBlockId(
        Constants.MAX_PARTITION_ID, Constants.MAX_TASK_ATTEMPT_ID, Constants.MAX_SEQUENCE_NO);
    bitmapBlockIds.addLong(expectedBlockId);
    resultBlockIds = shuffleTaskManager.getBlockIdsByPartitionId(Constants.MAX_PARTITION_ID, bitmapBlockIds);
    assertEquals(Roaring64NavigableMap.bitmapOf(expectedBlockId), resultBlockIds);
  }

  // copy from ClientUtils
  private Long getBlockId(long partitionId, long taskAttemptId, long atomicInt) {
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskAttemptId;
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String appId, int shuffleId, int expectedBlockNum) throws Exception {
    int retry = 0;
    while (true) {
      // remove flushed eventId to test timeout in commit
      if (shuffleFlushManager.getCommittedBlockIds(appId, shuffleId).getIntCardinality() == expectedBlockNum) {
        break;
      }
      Thread.sleep(1000);
      retry++;
      if (retry > 5) {
        fail("Timeout to flush data");
      }
    }
  }

  private ShufflePartitionedData createPartitionedData(int partitionId, int blockNum, int dataLength) {
    ShufflePartitionedBlock[] blocks = createBlock(blockNum, dataLength);
    return new ShufflePartitionedData(partitionId, blocks);
  }

  private ShufflePartitionedBlock[] createBlock(int num, int length) {
    ShufflePartitionedBlock[] blocks = new ShufflePartitionedBlock[num];
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks[i] = new ShufflePartitionedBlock(
          length, length, ChecksumUtils.getCrc32(buf), ATOMIC_INT.incrementAndGet(), 0, buf);
    }
    return blocks;
  }

  private void validate(String appId, int shuffleId, int partitionId, List<ShufflePartitionedBlock> blocks,
      String basePath) {
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Set<Long> remainIds = Sets.newHashSet();
    for (ShufflePartitionedBlock spb : blocks) {
      expectBlockIds.addLong(spb.getBlockId());
      remainIds.add(spb.getBlockId());
    }
    HdfsClientReadHandler handler = new HdfsClientReadHandler(appId, shuffleId, partitionId,
        100, 1, 10, 1000, expectBlockIds, processBlockIds, basePath, new Configuration());

    ShuffleDataResult sdr = handler.readShuffleData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matchNum = 0;
    for (ShufflePartitionedBlock block : blocks) {
      for (BufferSegment bs : bufferSegments) {
        if (bs.getBlockId() == block.getBlockId()) {
          assertEquals(block.getLength(), bs.getLength());
          assertEquals(block.getCrc(), bs.getCrc());
          matchNum++;
          break;
        }
      }
    }
    assertEquals(blocks.size(), matchNum);
  }
}
