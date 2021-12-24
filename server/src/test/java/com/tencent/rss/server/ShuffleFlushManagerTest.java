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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.util.StorageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShuffleFlushManagerTest extends HdfsTestBase {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);

  static {
    ShuffleServerMetrics.register();
  }

  private ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
  private String storageBasePath = HDFS_URI + "rss/test";

  @Before
  public void prepare() {
    shuffleServerConf.setString("rss.storage.basePath", storageBasePath);
    shuffleServerConf.setString("rss.storage.type", "HDFS");
    LogManager.getRootLogger().setLevel(Level.INFO);
  }

  @Test
  public void hadoopConfTest() {
    shuffleServerConf.setString("rss.server.hadoop.dfs.replication", "2");
    shuffleServerConf.setString("rss.server.hadoop.a.b", "value");
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null, null);
    assertEquals("2", manager.getHadoopConf().get("dfs.replication"));
    assertEquals("value", manager.getHadoopConf().get("a.b"));
  }

  @Test
  public void writeTest() throws Exception {
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null, null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", 1, 1, 1, null);
    List<ShufflePartitionedBlock> blocks1 = event1.getShuffleBlocks();
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event21 =
        createShuffleDataFlushEvent("appId1", 2, 2, 2, null);
    List<ShufflePartitionedBlock> blocks21 = event21.getShuffleBlocks();
    manager.addToFlushQueue(event21);
    ShuffleDataFlushEvent event22 =
        createShuffleDataFlushEvent("appId1", 2, 2, 2, null);
    List<ShufflePartitionedBlock> blocks22 = event22.getShuffleBlocks();
    manager.addToFlushQueue(event22);
    // wait for write data
    waitForFlush(manager, "appId1", 1, 5);
    waitForFlush(manager, "appId1", 2, 10);
    validate("appId1", 1, 1, blocks1, 1, storageBasePath);
    assertEquals(blocks1.size(), manager.getCommittedBlockIds("appId1", 1).getLongCardinality());

    blocks21.addAll(blocks22);
    validate("appId1", 2, 2, blocks21, 1, storageBasePath);
    assertEquals(blocks21.size(), manager.getCommittedBlockIds("appId1", 2).getLongCardinality());
  }

  @Test
  public void complexWriteTest() throws Exception {
    shuffleServerConf.setString("rss.server.flush.handler.expired", "3");
    List<ShufflePartitionedBlock> expectedBlocks = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents1 = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents2 = Lists.newArrayList();
    ShuffleFlushManager manager = new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null, null);
    for (int i = 0; i < 30; i++) {
      ShuffleDataFlushEvent flushEvent1 = createShuffleDataFlushEvent("appId4", 1, 1, 1, null);
      ShuffleDataFlushEvent flushEvent2 = createShuffleDataFlushEvent("appId4", 1, 1, 1, null);
      expectedBlocks.addAll(flushEvent1.getShuffleBlocks());
      expectedBlocks.addAll(flushEvent2.getShuffleBlocks());
      flushEvents1.add(flushEvent1);
      flushEvents2.add(flushEvent2);
    }
    Thread flushThread1 = new Thread(() -> {
      for (ShuffleDataFlushEvent event : flushEvents1) {
        manager.addToFlushQueue(event);
      }
    });

    Thread flushThread2 = new Thread(() -> {
      for (ShuffleDataFlushEvent event : flushEvents2) {
        manager.addToFlushQueue(event);
      }
    });
    flushThread1.start();
    flushThread2.start();
    flushThread1.join();
    flushThread2.join();

    waitForFlush(manager, "appId4", 1, 300);
    validate("appId4", 1, 1, expectedBlocks, 1, storageBasePath);
  }

  @Test
  public void clearTest() throws Exception {
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null, null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", 1, 0, 1, null);
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event2 =
        createShuffleDataFlushEvent("appId2", 1, 0, 1, null);
    manager.addToFlushQueue(event2);
    waitForFlush(manager, "appId1", 1, 5);
    waitForFlush(manager, "appId2", 1, 5);
    assertEquals(5, manager.getCommittedBlockIds("appId1", 1).getLongCardinality());
    assertEquals(5, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(2, manager.getHandlers().size());
    FileStatus[] fileStatus = fs.listStatus(new Path(storageBasePath + "/appId1/"));
    assertTrue(fileStatus.length > 0);
    manager.removeResources("appId1");
    try {
      fs.listStatus(new Path(storageBasePath + "/appId1/"));
      fail("Exception should be thrown");
    } catch (FileNotFoundException fnfe) {
      // expected exception
    }

    assertEquals(0, manager.getCommittedBlockIds("appId1", 1).getLongCardinality());
    assertEquals(5, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(1, manager.getHandlers().size());
    manager.removeResources("appId2");
    assertEquals(0, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(0, manager.getHandlers().size());
  }

  @Test
  public void clearLocalTest() throws Exception {
    TemporaryFolder tmpDir = new TemporaryFolder();
    tmpDir.create();
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setString("rss.storage.basePath", tmpDir.getRoot().getAbsolutePath());
    serverConf.setString("rss.storage.type", "LOCALFILE");
    ShuffleFlushManager manager =
        new ShuffleFlushManager(serverConf, "shuffleServerId", null, null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", 1, 0, 1, null);
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event2 =
        createShuffleDataFlushEvent("appId2", 1, 0, 1, null);
    manager.addToFlushQueue(event2);
    waitForFlush(manager, "appId1", 1, 5);
    waitForFlush(manager, "appId2", 1, 5);
    assertEquals(5, manager.getCommittedBlockIds("appId1", 1).getLongCardinality());
    assertEquals(5, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(2, manager.getHandlers().size());
    File file = new File(tmpDir.getRoot(), "appId1");
    assertTrue(file.exists());
    manager.removeResources("appId1");
    assertFalse(file.exists());
    ShuffleDataFlushEvent event3 =
        createShuffleDataFlushEvent("app1", 1, 0, 1, () -> { return  false; });
    manager.addToFlushQueue(event3);
    Thread.sleep(1000);
    assertEquals(0, manager.getCommittedBlockIds("appId1", 1).getLongCardinality());
    assertEquals(5, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(1, manager.getHandlers().size());
    manager.removeResources("appId2");
    assertEquals(0, manager.getCommittedBlockIds("appId2", 1).getLongCardinality());
    assertEquals(0, manager.getHandlers().size());
    tmpDir.delete();
  }

  private void waitForFlush(ShuffleFlushManager manager,
      String appId, int shuffleId, int expectedBlockNum) throws Exception {
    int retry = 0;
    int size = 0;
    do {
      Thread.sleep(500);
      if (retry > 100) {
        fail("Unexpected flush process");
      }
      retry++;
      size = manager.getCommittedBlockIds(appId, shuffleId).getIntCardinality();
    } while (size < expectedBlockNum);
  }

  private ShuffleDataFlushEvent createShuffleDataFlushEvent(
      String appId, int shuffleId, int startPartition, int endPartition, Supplier<Boolean> isValid) {
    List<ShufflePartitionedBlock> spbs = createBlock(5, 32);
    return new ShuffleDataFlushEvent(ATOMIC_LONG.getAndIncrement(),
        appId, shuffleId, startPartition, endPartition, 1, spbs, isValid, null);
  }

  private List<ShufflePartitionedBlock> createBlock(int num, int length) {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks.add(new ShufflePartitionedBlock(
          length, length, ChecksumUtils.getCrc32(buf),
          ATOMIC_INT.incrementAndGet(), 0, buf));
    }
    return blocks;
  }

  private void validate(String appId, int shuffleId, int partitionId, List<ShufflePartitionedBlock> blocks,
      int partitionNumPerRange, String basePath) {
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Set<Long> remainIds = Sets.newHashSet();
    for (ShufflePartitionedBlock spb : blocks) {
      blockIdBitmap.addLong(spb.getBlockId());
      remainIds.add(spb.getBlockId());
    }
    HdfsClientReadHandler handler = new HdfsClientReadHandler(
        appId,
        shuffleId,
        partitionId,
        100,
        partitionNumPerRange,
        10,
        blocks.size() * 32,
        basePath,
        new Configuration());
    ShuffleDataResult sdr = null;
    int matchNum = 0;
    sdr = handler.readShuffleData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    for (ShufflePartitionedBlock block : blocks) {
      for (BufferSegment bs : bufferSegments) {
        if (bs.getBlockId() == block.getBlockId()) {
          matchNum++;
          break;
        }
      }
    }
    for (BufferSegment bs : bufferSegments) {
      remainIds.remove(bs.getBlockId());
    }
    assertEquals(blocks.size(), matchNum);
  }

  @Test
  public void processPendingEventsTest() {
    try {
      TemporaryFolder processEventsTmpdir = new TemporaryFolder();
      processEventsTmpdir.create();
      shuffleServerConf.set(RssBaseConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.toString());
      shuffleServerConf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, processEventsTmpdir.getRoot().getAbsolutePath());
      shuffleServerConf.set(ShuffleServerConf.DISK_CAPACITY, 100L);
      shuffleServerConf.set(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC, 5L);
      shuffleServerConf.set(ShuffleServerConf.HDFS_BASE_PATH, "test");
      shuffleServerConf.set(ShuffleServerConf.MULTI_STORAGE_ENABLE, true);
      MultiStorageManager storageManager = new MultiStorageManager(shuffleServerConf, "");
      ShuffleFlushManager manager =
          new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null, storageManager);
      ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(1, "1", 1, 1,1, 100, null, null, null);
      assertEquals(0, manager.getPendingEventsSize());
      manager.addPendingEvents(event);
      Thread.sleep(1000);
      assertEquals(0, manager.getPendingEventsSize());
      do {
        Thread.sleep(1 * 1000);
      } while(manager.getEventNumInFlush() != 0);
      List<ShufflePartitionedBlock> blocks = Lists.newArrayList(new ShufflePartitionedBlock(100, 1000, 1, 1, 1L, null));
      ShuffleDataFlushEvent bigEvent = new ShuffleDataFlushEvent(1, "1", 1, 1, 1, 100, blocks, null, null);
      storageManager.updateWriteEvent(bigEvent);
      manager.addPendingEvents(event);
      manager.addPendingEvents(event);
      manager.addPendingEvents(event);
      Thread.sleep(1000);
      assertTrue(2 <= manager.getPendingEventsSize());
      int eventNum = (int) ShuffleServerMetrics.counterTotalDroppedEventNum.get();
      Thread.sleep(6 * 1000);
      assertEquals(eventNum + 3, (int) ShuffleServerMetrics.counterTotalDroppedEventNum.get());
      assertEquals(0, manager.getPendingEventsSize());
      processEventsTmpdir.delete();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
