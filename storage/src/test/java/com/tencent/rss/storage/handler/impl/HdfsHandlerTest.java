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

package com.tencent.rss.storage.handler.impl;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class HdfsHandlerTest extends HdfsTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void initTest() throws IOException {
    String basePath = HDFS_URI + "test_base";
    new HdfsShuffleWriteHandler("appId", 0, 0, 0, basePath, "test", conf);
    Path path = new Path(basePath);
    assertTrue(fs.isDirectory(path));
  }

  @Test
  public void writeTest() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "writeTest";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 1, 1, 1, basePath, "test", conf);
    List<ShufflePartitionedBlock> blocks = new LinkedList<>();
    List<Long> expectedBlockId = new LinkedList<>();
    List<byte[]> expectedData = new LinkedList<>();
    List<FileBasedShuffleSegment> expectedIndex = new LinkedList<>();

    int pos = 0;
    for (int i = 1; i < 13; ++i) {
      byte[] buf = new byte[i * 8];
      new Random().nextBytes(buf);
      expectedData.add(buf);
      blocks.add(new ShufflePartitionedBlock(i * 8, i * 8, i, i, 0, buf));
      expectedBlockId.add(Long.valueOf(i));
      expectedIndex.add(new FileBasedShuffleSegment(i, pos, i * 8, i * 8, i, 0));
      pos += i * 8;
    }
    writeHandler.write(blocks);

    compareDataAndIndex("appId", 1, 1, basePath, expectedData, expectedBlockId);

    // append the exist data and index files
    List<ShufflePartitionedBlock> blocksAppend = new LinkedList<>();
    for (int i = 13; i < 23; ++i) {
      byte[] buf = new byte[i * 8];
      new Random().nextBytes(buf);
      expectedData.add(buf);
      expectedBlockId.add(Long.valueOf(i));
      blocksAppend.add(new ShufflePartitionedBlock(i * 8, i * 8, i, i, i, buf));
      expectedIndex.add(new FileBasedShuffleSegment(i, pos, i * 8, i * 8, i, i));
      pos += i * 8;
    }
    writeHandler =
        new HdfsShuffleWriteHandler("appId", 1, 1, 1, basePath, "test", conf);
    writeHandler.write(blocksAppend);

    compareDataAndIndex("appId", 1, 1, basePath, expectedData, expectedBlockId);
  }

  private void compareDataAndIndex(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      List<byte[]> expectedData,
      List<Long> expectedBlockId) throws IllegalStateException {
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (long blockId : expectedBlockId) {
      blockIdBitmap.addLong(blockId);
    }
    // read directly and compare
    HdfsClientReadHandler readHandler = new HdfsClientReadHandler(
        appId, shuffleId, partitionId, 100, 1, 10,
        10000, basePath, new Configuration());
    try {
      List<ByteBuffer> actual = readData(readHandler, Sets.newHashSet(expectedBlockId));
      compareBytes(expectedData, actual);
    } finally {
      readHandler.close();
    }
  }

  private List<ByteBuffer> readData(HdfsClientReadHandler handler, Set<Long> blockIds) throws IllegalStateException {
    ShuffleDataResult sdr = handler.readShuffleData(0);
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    List<ByteBuffer> result = Lists.newArrayList();
    for (BufferSegment bs : bufferSegments) {
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(sdr.getData(), bs.getOffset(), data, 0, bs.getLength());
      result.add(ByteBuffer.wrap(data));
    }
    return result;
  }

}

