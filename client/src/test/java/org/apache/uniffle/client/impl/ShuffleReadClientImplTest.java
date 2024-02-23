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

package org.apache.uniffle.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.TestUtils;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.HadoopTestBase;
import org.apache.uniffle.storage.handler.impl.HadoopShuffleWriteHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

public class ShuffleReadClientImplTest extends HadoopTestBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  private ShuffleServerInfo ssi1 = new ShuffleServerInfo("host1-0", "host1", 0);
  private ShuffleServerInfo ssi2 = new ShuffleServerInfo("host2-0", "host2", 0);

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder() {
    return ShuffleClientFactory.newReadBuilder()
        .storageType(StorageType.HDFS.name())
        .appId("appId")
        .shuffleId(0)
        .partitionId(1)
        .indexReadLimit(100)
        .partitionNumPerRange(1)
        .partitionNum(10)
        .readBufferSize(1000)
        .shuffleServerInfoList(Lists.newArrayList(ssi1));
  }

  @Test
  public void readTest1() throws Exception {
    String basePath = HDFS_URI + "clientReadTest1";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    blockIdBitmap.addLong(BlockId.getBlockId(0, 0, Constants.MAX_TASK_ATTEMPT_ID - 1));
    taskIdBitmap.addLong(Constants.MAX_TASK_ATTEMPT_ID - 1);
    readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
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
  public void readTest2() throws Exception {
    String basePath = HDFS_URI + "clientReadTest2";
    HadoopShuffleWriteHandler writeHandler1 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);
    HadoopShuffleWriteHandler writeHandler2 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi2.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler1, 2, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler2, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(ssi1, ssi2))
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest3() throws Exception {
    String basePath = HDFS_URI + "clientReadTest3";
    HadoopShuffleWriteHandler writeHandler1 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);
    HadoopShuffleWriteHandler writeHandler2 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi2.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler1, 2, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler2, 2, 30, 0, expectedData, blockIdBitmap);

    // duplicate file created, it should be used in product environment
    String shuffleFolder = basePath + "/appId/0/0-1";
    FileUtil.copy(
        fs,
        new Path(shuffleFolder + "/" + ssi1.getId() + "_0.data"),
        fs,
        new Path(basePath + "/" + ssi1.getId() + ".cp.data"),
        false,
        conf);
    FileUtil.copy(
        fs,
        new Path(shuffleFolder + "/" + ssi1.getId() + "_0.index"),
        fs,
        new Path(basePath + "/" + ssi1.getId() + ".cp.index"),
        false,
        conf);
    FileUtil.copy(
        fs,
        new Path(shuffleFolder + "/" + ssi2.getId() + "_0.data"),
        fs,
        new Path(basePath + "/" + ssi2.getId() + ".cp.data"),
        false,
        conf);
    FileUtil.copy(
        fs,
        new Path(shuffleFolder + "/" + ssi2.getId() + "_0.index"),
        fs,
        new Path(basePath + "/" + ssi2.getId() + ".cp.index"),
        false,
        conf);

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(ssi1, ssi2))
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "clientReadTest4";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    Path dataFile = new Path(basePath + "/appId/0/0-1/" + ssi1.getId() + "_0.data");
    // data file is deleted after readClient checkExpectedBlockIds
    fs.delete(dataFile, true);

    assertNull(readClient.readShuffleBlockData());
    try {
      fs.listStatus(dataFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
      // ignore
    }

    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Blocks read inconsistent: expected"));
    }
    readClient.close();
  }

  @Test
  public void readTest5() throws Exception {
    String basePath = HDFS_URI + "clientReadTest5";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    // index file is deleted after iterator initialization, it should be ok, all index infos are
    // read already
    Path indexFile = new Path(basePath + "/appId/0/0-1/" + ssi1.getId() + "_0.index");
    fs.delete(indexFile, true);
    readClient.close();

    assertNull(readClient.readShuffleBlockData());
  }

  @Test
  public void readTest7() throws Exception {
    String basePath = HDFS_URI + "clientReadTest7";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData1 = Maps.newHashMap();
    Map<Long, byte[]> expectedData2 = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 10, 30, 0, expectedData1, blockIdBitmap1);

    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    writeTestData(writeHandler, 10, 30, 0, expectedData2, blockIdBitmap2);

    writeTestData(writeHandler, 10, 30, 0, expectedData1, blockIdBitmap1);
    ShuffleReadClientImpl readClient1 =
        baseReadBuilder()
            .partitionId(0)
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap1)
            .taskIdBitmap(taskIdBitmap)
            .build();

    final ShuffleReadClientImpl readClient2 =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap2)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient1, expectedData1);
    readClient1.checkProcessedBlockIds();
    readClient1.close();

    TestUtils.validateResult(readClient2, expectedData2);
    readClient2.checkProcessedBlockIds();
    readClient2.close();
  }

  @Test
  public void readTest8() throws Exception {
    String basePath = HDFS_URI + "clientReadTest8";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    ShuffleReadClientImpl readClient2 =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(ssi1, ssi2))
            .build();
    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32((ByteBuffer) any())).thenReturn(-1L);
      try {
        ByteBuffer bb = readClient.readShuffleBlockData().getByteBuffer();
        while (bb != null) {
          bb = readClient.readShuffleBlockData().getByteBuffer();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Unexpected crc value"));
      }

      CompressedShuffleBlock block = readClient2.readShuffleBlockData();
      assertNull(block);
    }
    readClient.close();
    readClient2.close();
  }

  @Test
  public void readTest9() {
    // empty data
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionNumPerRange(2)
            .basePath("basePath")
            .blockIdBitmap(Roaring64NavigableMap.bitmapOf())
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf())
            .build();
    assertNull(readClient.readShuffleBlockData());
    readClient.checkProcessedBlockIds();
  }

  @Test
  public void readTest10() throws Exception {
    String basePath = HDFS_URI + "clientReadTest10";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    Roaring64NavigableMap wrongBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    LongIterator iter = blockIdBitmap.getLongIterator();
    while (iter.hasNext()) {
      BlockId blockId = BlockId.fromLong(iter.next());
      wrongBlockIdBitmap.addLong(
          BlockId.getBlockId(blockId.sequenceNo, blockId.partitionId + 1, blockId.taskAttemptId));
    }

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .partitionId(0)
            .partitionNumPerRange(2)
            .basePath(basePath)
            .blockIdBitmap(wrongBlockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    assertNull(readClient.readShuffleBlockData());
    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Blocks read inconsistent:"));
    }
  }

  @Test
  public void readTest11() throws Exception {
    String basePath = HDFS_URI + "clientReadTest11";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 10, 30, 0, expectedData, blockIdBitmap);
    // test with different indexReadLimit to validate result
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .indexReadLimit(1)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient =
        baseReadBuilder()
            .indexReadLimit(2)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient =
        baseReadBuilder()
            .indexReadLimit(3)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient =
        baseReadBuilder()
            .indexReadLimit(10)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient =
        baseReadBuilder()
            .indexReadLimit(11)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest12() throws Exception {
    String basePath = HDFS_URI + "clientReadTest12";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 2, Maps.newHashMap(), blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 1, expectedData, blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    assertEquals(15, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest13() throws Exception {
    String basePath = HDFS_URI + "clientReadTest13";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 3);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    // test case: data generated by speculation task without report result
    writeTestData(writeHandler, 5, 30, 1, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    // test case: data generated by speculation task with report result
    writeTestData(writeHandler, 5, 30, 2, Maps.newHashMap(), blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 3, expectedData, blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    assertEquals(20, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest14() throws Exception {
    String basePath = HDFS_URI + "clientReadTest14";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 2);
    writeDuplicatedData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 1, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    writeTestData(writeHandler, 5, 30, 2, expectedData, blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    assertEquals(15, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest15() throws Exception {
    String basePath = HDFS_URI + "clientReadTest15";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi1.getId(), conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 0, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    writeTestData(writeHandler, 5, 30, 0, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 0, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    TestUtils.validateResult(readClient, expectedData);
    assertEquals(25, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  private void writeTestData(
      HadoopShuffleWriteHandler writeHandler,
      int num,
      int length,
      long taskAttemptId,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap)
      throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = BlockId.getBlockId(ATOMIC_INT.getAndIncrement(), 0, taskAttemptId);
      blocks.add(
          new ShufflePartitionedBlock(
              length, length, ChecksumUtils.getCrc32(buf), blockId, taskAttemptId, buf));
      expectedData.put(blockId, buf);
      blockIdBitmap.addLong(blockId);
    }
    writeHandler.write(blocks);
  }

  private void writeDuplicatedData(
      HadoopShuffleWriteHandler writeHandler,
      int num,
      int length,
      long taskAttemptId,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap)
      throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = BlockId.getBlockId(ATOMIC_INT.getAndIncrement(), 0, taskAttemptId);
      ShufflePartitionedBlock spb =
          new ShufflePartitionedBlock(
              length, length, ChecksumUtils.getCrc32(buf), blockId, taskAttemptId, buf);
      blocks.add(spb);
      blocks.add(spb);
      expectedData.put(blockId, buf);
      blockIdBitmap.addLong(blockId);
    }
    writeHandler.write(blocks);
  }
}
