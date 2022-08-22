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

package org.apache.uniffle.storage.handler.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalFileHandlerTest {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);

  @Test
  public void writeTest() throws Exception {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String[] basePaths = new String[]{dataDir1.getAbsolutePath(),
        dataDir2.getAbsolutePath()};
    final LocalFileWriteHandler writeHandler1 = new LocalFileWriteHandler("appId", 0, 1, 1,
        basePaths[0], "pre");
    final LocalFileWriteHandler writeHandler2 = new LocalFileWriteHandler("appId", 0, 2, 2,
        basePaths[0], "pre");

    String possiblePath1 = ShuffleStorageUtils.getFullShuffleDataFolder(dataDir1.getAbsolutePath(),
        ShuffleStorageUtils.getShuffleDataPath("appId", 0, 1, 1));
    String possiblePath2 = ShuffleStorageUtils.getFullShuffleDataFolder(dataDir2.getAbsolutePath(),
        ShuffleStorageUtils.getShuffleDataPath("appId", 0, 1, 1));
    assertTrue(writeHandler1.getBasePath().endsWith(possiblePath1)
        || writeHandler1.getBasePath().endsWith(possiblePath2));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    final Set<Long> expectedBlockIds1 = Sets.newHashSet();
    final Set<Long> expectedBlockIds2 = Sets.newHashSet();

    writeTestData(writeHandler1, 1, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 2, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 3, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 4, 32, expectedData, expectedBlockIds1);

    writeTestData(writeHandler2, 3, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 3, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 2, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 1, 32, expectedData, expectedBlockIds2);

    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.storage.basePath", dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath());
    LocalFileServerReadHandler readHandler1 = new LocalFileServerReadHandler(
        "appId", 0, 1, 1, 10, dataDir1.getAbsolutePath());
    LocalFileServerReadHandler readHandler2 = new LocalFileServerReadHandler(
        "appId", 0, 2, 1, 10, dataDir1.getAbsolutePath());

    validateResult(readHandler1, expectedBlockIds1, expectedData);
    validateResult(readHandler2, expectedBlockIds2, expectedData);

    // after first read, write more data
    writeTestData(writeHandler1, 1, 32, expectedData, expectedBlockIds1);
    // new data should be read
    validateResult(readHandler1, expectedBlockIds1, expectedData);

    File targetDataFile = new File(possiblePath1, "pre.data");
    ShuffleIndexResult shuffleIndexResult = readIndex(readHandler1);
    assertFalse(shuffleIndexResult.isEmpty());
    List<ShuffleDataResult> shuffleDataResults = readData(readHandler1, shuffleIndexResult);
    assertFalse(shuffleDataResults.isEmpty());
    targetDataFile.delete();
    shuffleDataResults = readData(readHandler1, shuffleIndexResult);
    for (ShuffleDataResult shuffleData : shuffleDataResults) {
      assertEquals(0, shuffleData.getData().length);
      assertTrue(shuffleData.isEmpty());
    }
  }

  @Test
  public void writeBigDataTest() throws IOException  {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File writeFile = new File(tmpDir, "writetest");
    LocalFileWriter writer = new LocalFileWriter(writeFile);
    int  size = Integer.MAX_VALUE / 100;
    byte[] data = new byte[size];
    for (int i = 0; i < 200; i++) {
      writer.writeData(data);
    }
    long totalSize = 200L * size;
    assertEquals(writer.nextOffset(), totalSize);
  }


  private void writeTestData(
      ShuffleWriteHandler writeHandler,
      int num, int length,
      Map<Long, byte[]> expectedData,
      Set<Long> expectedBlockIds) throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.incrementAndGet();
      blocks.add(new ShufflePartitionedBlock(length, length, ChecksumUtils.getCrc32(buf), blockId, 100,
          buf));
      expectedData.put(blockId, buf);
      expectedBlockIds.add(blockId);
    }
    writeHandler.write(blocks);
  }

  protected void validateResult(ServerReadHandler readHandler, Set<Long> expectedBlockIds,
      Map<Long, byte[]> expectedData) {
    List<ShuffleDataResult> shuffleDataResults = readAll(readHandler);
    Set<Long> actualBlockIds = Sets.newHashSet();
    for (ShuffleDataResult sdr : shuffleDataResults) {
      byte[] buffer = sdr.getData();
      List<BufferSegment> bufferSegments = sdr.getBufferSegments();

      for (BufferSegment bs : bufferSegments) {
        byte[] data = new byte[bs.getLength()];
        System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
        assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
        assertArrayEquals(expectedData.get(bs.getBlockId()), data);
        actualBlockIds.add(bs.getBlockId());
      }
    }
    assertEquals(expectedBlockIds, actualBlockIds);
  }

  private List<ShuffleDataResult> readAll(ServerReadHandler readHandler) {
    ShuffleIndexResult shuffleIndexResult = readIndex(readHandler);
    return readData(readHandler, shuffleIndexResult);
  }

  private ShuffleIndexResult readIndex(ServerReadHandler readHandler) {
    ShuffleIndexResult shuffleIndexResult = readHandler.getShuffleIndex();
    return shuffleIndexResult;
  }

  private List<ShuffleDataResult> readData(ServerReadHandler readHandler, ShuffleIndexResult shuffleIndexResult) {
    List<ShuffleDataResult> shuffleDataResults = Lists.newLinkedList();
    if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
      return shuffleDataResults;
    }

    List<ShuffleDataSegment> shuffleDataSegments =
        RssUtils.transIndexDataToSegments(shuffleIndexResult, 32);

    for (ShuffleDataSegment shuffleDataSegment : shuffleDataSegments) {
      byte[] shuffleData =
          readHandler.getShuffleData(shuffleDataSegment.getOffset(), shuffleDataSegment.getLength()).getData();
      ShuffleDataResult sdr = new ShuffleDataResult(shuffleData, shuffleDataSegment.getBufferSegments());
      shuffleDataResults.add(sdr);
    }

    return shuffleDataResults;
  }

}
