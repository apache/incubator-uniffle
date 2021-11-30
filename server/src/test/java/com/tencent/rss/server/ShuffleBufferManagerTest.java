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

package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.RangeMap;
import com.google.common.io.Files;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class ShuffleBufferManagerTest {

  static {
    ShuffleServerMetrics.register();
  }

  private ShuffleBufferManager shuffleBufferManager;
  private ShuffleFlushManager mockShuffleFlushManager;
  private AtomicInteger atomicInt = new AtomicInteger(0);

  @Before
  public void setUp() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString("rss.server.buffer.capacity", "500");
    conf.setString("rss.server.buffer.spill.threshold", "256");
    conf.setString("rss.server.partition.buffer.size", "96");
    mockShuffleFlushManager = mock(ShuffleFlushManager.class);
    shuffleBufferManager = new ShuffleBufferManager(conf, mockShuffleFlushManager);
  }

  @Test
  public void registerBufferTest() {
    String appId = "registerBufferTest";
    int shuffleId = 1;

    StatusCode sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(StatusCode.SUCCESS, sc);
    sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = shuffleBufferManager.getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void cacheShuffleDataTest() {
    String appId = "cacheShuffleDataTest";
    int shuffleId = 1;

    int startPartitionNum = (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get();
    StatusCode sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId + 1, 0, 1);
    assertEquals(startPartitionNum + 1, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 100, 101);
    assertEquals(startPartitionNum + 2, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(startPartitionNum + 3, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = shuffleBufferManager.getBufferPool();
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(48, buffer.getSize());
    assertEquals(48, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(96, buffer.getSize());
    assertEquals(96, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    assertEquals(0, buffer.getSize());
    assertEquals(129, shuffleBufferManager.getUsedMemory());
    assertEquals(129, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 95));
    assertEquals(0, buffer.getSize());
    assertEquals(256, shuffleBufferManager.getUsedMemory());
    assertEquals(256, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 22));
    assertEquals(54, buffer.getSize());
    assertEquals(310, shuffleBufferManager.getUsedMemory());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());

    // now buffer should be full
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 159));
    verify(mockShuffleFlushManager, times(3)).addToFlushQueue(any());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // size won't be reduce which should be processed by flushManager, reset buffer size to 0
    shuffleBufferManager.resetSize();
    shuffleBufferManager.removeBuffer(appId);
    assertEquals(startPartitionNum, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 4, 5);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    shuffleBufferManager.registerBuffer("appId1", shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer("appId2", shuffleId, 0, 1);

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 65));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(4, 65));
    shuffleBufferManager.cacheShuffleData(appId, 2, false, createData(0, 32));
    shuffleBufferManager.cacheShuffleData("appId1", shuffleId, false, createData(0, 32));
    assertEquals(322, shuffleBufferManager.getUsedMemory());
    assertEquals(194, shuffleBufferManager.getInFlushSize());
    shuffleBufferManager.cacheShuffleData("appId2", shuffleId, false, createData(0, 65));
    assertEquals(419, shuffleBufferManager.getUsedMemory());
    assertEquals(291, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(6)).addToFlushQueue(any());
  }

  @Test
  public void cacheShuffleDataWithPreAllocationTest() {
    String appId = "cacheShuffleDataWithPreAllocationTest";
    int shuffleId = 1;

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    // pre allocate memory
    shuffleBufferManager.requireMemory(48, true);
    assertEquals(48, shuffleBufferManager.getUsedMemory());
    assertEquals(48, shuffleBufferManager.getPreAllocatedSize());
    // receive data with preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 16));
    assertEquals(48, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // release memory
    shuffleBufferManager.releaseMemory(48, false, false);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // receive data without preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 17));
    assertEquals(49, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // single buffer flush
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());
    // release memory
    shuffleBufferManager.releaseMemory(49, false, false);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());

    // release memory with preAllocation
    shuffleBufferManager.requireMemory(16, true);
    shuffleBufferManager.releaseMemory(16, false, true);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());

    // pre allocate all memory
    shuffleBufferManager.requireMemory(500, true);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(500, shuffleBufferManager.getPreAllocatedSize());

    // no buffer if data without pre allocation
    StatusCode sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(1, 16));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // actual data size < spillThreshold, won't flush
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(1, 16));
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(452, shuffleBufferManager.getPreAllocatedSize());
    // no flush happen
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    // actual data size > spillThreshold, flush
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 177));
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(243, shuffleBufferManager.getPreAllocatedSize());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());
  }

  @Test
  public void bufferSizeTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir = new File(tmpDir, "data");
    conf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    conf.setString("rss.server.buffer.capacity", "500");
    conf.setString("rss.server.buffer.spill.threshold", "256");
    conf.setString("rss.server.partition.buffer.size", "96");

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    ShuffleFlushManager shuffleFlushManager = new ShuffleFlushManager(conf, "serverId", mockShuffleServer, null);
    shuffleBufferManager = new ShuffleBufferManager(conf, shuffleFlushManager);

    when(mockShuffleServer
        .getShuffleFlushManager())
        .thenReturn(shuffleFlushManager);
    when(mockShuffleServer
        .getShuffleBufferManager())
        .thenReturn(shuffleBufferManager);

    String appId = "bufferSizeTest";
    int shuffleId = 1;

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 4, 5);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 6, 7);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 8, 9);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 10, 11);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(48, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(96, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 3);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 64));
    assertEquals(96, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 64));
    assertEquals(192, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(4, 64));
    assertEquals(288, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(6, 64));
    assertEquals(384, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(8, 64));
    assertEquals(480, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(10, 64));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 6);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.registerBuffer("bufferSizeTest1", shuffleId, 0, 1);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 32));
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData("bufferSizeTest1", shuffleId, false, createData(0, 32));
    assertEquals(128, shuffleBufferManager.getUsedMemory());
    assertEquals(2, shuffleBufferManager.getBufferPool().keySet().size());
    shuffleBufferManager.removeBuffer(appId);
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    assertEquals(1, shuffleBufferManager.getBufferPool().keySet().size());
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String appId, int shuffleId, int expectedBlockNum) throws Exception {
    int retry = 0;
    long committedCount = 0;
    do {
      committedCount = shuffleFlushManager.getCommittedBlockIds(appId, shuffleId).getLongCardinality();
      if (committedCount < expectedBlockNum) {
        Thread.sleep(500);
      }
      retry++;
      if (retry > 10) {
        fail("Flush data time out");
      }
    } while (committedCount < expectedBlockNum);
  }

  private ShufflePartitionedData createData(int partitionId, int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(
        len, len, 1, atomicInt.incrementAndGet(), 0, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(partitionId, new ShufflePartitionedBlock[]{block});
    return data;
  }
}
