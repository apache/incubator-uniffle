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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsShuffleHandlerTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class MultiStorageHdfsShuffleReadHandlerTest extends HdfsShuffleHandlerTestBase {

  @Test
  public void test() {
    try {
      String basePath = HDFS_URI + "test_base";
      int readBufferSize = 7;
      Path testPath = new Path(basePath + "/app1/0/test");
      fs.mkdirs(testPath);
      Path dataPath = new Path(basePath + "/app1/0/test/1-3.data");
      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);
      // write data and record meta data
      Map<Integer, Integer> partitionSliceNum = Maps.newHashMap();
      Map<Long, byte[]> expectedBlockIdData = Maps.newHashMap();
      Map<Integer, List<ShufflePartitionedBlock>> expectedBlocks = Maps.newHashMap();
      Map<Integer, List<FileBasedShuffleSegment>> expectedIndexSegments = Maps.newHashMap();
      int partitionNum = 5;
      int dataFileLen = 0;
      int totalIndexSegmentNum = 0;
      List<ShuffleIndexHeader.Entry> indexHeaderEntries = Lists.newArrayList();
      ByteBuffer headerContentBuf = ByteBuffer.allocate(
          (int)ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum) - ShuffleStorageUtils.getHeaderCrcLen());
      headerContentBuf.putInt(partitionNum);
      for (int partitionId = 1; partitionId <= 9; partitionId += 2) {
        int expectTotalBlockNum = new Random().nextInt(37) + 1;
        int blockSize = new Random().nextInt(7) + 1;
        writeTestData(writer, partitionId, expectTotalBlockNum, blockSize, 0,
            expectedBlockIdData, expectedBlocks, expectedIndexSegments, partitionId <= 5);
        int sliceNum = calcExpectedSegmentNum(expectTotalBlockNum, blockSize, readBufferSize);
        partitionSliceNum.put(partitionId, sliceNum);
        List<FileBasedShuffleSegment> segments = expectedIndexSegments.get(partitionId);
        totalIndexSegmentNum += segments.size();
        long indexLen = segments.size() * FileBasedShuffleSegment.SEGMENT_SIZE;
        long dataLen = expectTotalBlockNum * blockSize;
        headerContentBuf.putInt(partitionId);
        headerContentBuf.putLong(indexLen);
        headerContentBuf.putLong(dataLen);
        dataFileLen += dataLen;
        indexHeaderEntries.add(new ShuffleIndexHeader.Entry(partitionId, indexLen, dataLen));
      }
      headerContentBuf.flip();
      long crc =  ChecksumUtils.getCrc32(headerContentBuf);
      ShuffleIndexHeader shuffleIndexHeader = new ShuffleIndexHeader(partitionNum, indexHeaderEntries, crc);
      writer.close();

      //  write index
      Path indexPath = new Path(basePath + "/app1/0/test/1-3.index");
      writer = new HdfsFileWriter(indexPath, conf);
      int indexFileLen =
          shuffleIndexHeader.getHeaderLen() + totalIndexSegmentNum * FileBasedShuffleSegment.SEGMENT_SIZE;
      ByteBuffer byteBuffer =  ByteBuffer.allocate(indexFileLen);
      byteBuffer.putInt(shuffleIndexHeader.getPartitionNum());
      for (ShuffleIndexHeader.Entry entry : shuffleIndexHeader.getIndexes()) {
        byteBuffer.putInt(entry.getPartitionId());
        byteBuffer.putLong(entry.getPartitionIndexLength());
        byteBuffer.putLong(entry.getPartitionDataLength());
      }
      byteBuffer.putLong(crc);
      for (int partitionId = 1; partitionId <= 9; partitionId += 2) {
        List<FileBasedShuffleSegment> fileBasedShuffleSegments = expectedIndexSegments.get(partitionId);
        for (FileBasedShuffleSegment segment : fileBasedShuffleSegments) {
          byteBuffer.putLong(segment.getOffset());
          byteBuffer.putInt(segment.getLength());
          byteBuffer.putInt(segment.getUncompressLength());
          if (partitionId <= 5) {
            byteBuffer.putLong(segment.getCrc());
            byteBuffer.putLong(segment.getBlockId());
            byteBuffer.putLong(segment.getTaskAttemptId());
          }
        }
      }
      byteBuffer.flip();
      writer.writeData(byteBuffer);
      writer.close();

      for (int partitionId = 1; partitionId <= 9; partitionId += 2) {
        String fileNamePrefix = basePath + "/app1/0/test/1-3";
        Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
        Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
        HdfsShuffleReadHandler handler = new UploadedStorageHdfsShuffleReadHandler(
            "app1", 0, partitionId, fileNamePrefix, readBufferSize,
            expectBlockIds, processBlockIds, conf);

        int sliceNum = partitionSliceNum.get(partitionId);
        byte[] expectedDataBuf = new byte[0];
        for (ShufflePartitionedBlock spb : expectedBlocks.get(partitionId)) {
          Bytes.concat(expectedDataBuf, spb.getData());
          expectBlockIds.addLong(spb.getBlockId());
        }

        if (partitionId <= 5) {
          byte[] actualDataBuf = new byte[0];
          for (int i = 0; i < sliceNum; ++i) {
            ShuffleDataResult shuffleDataResult = handler.readShuffleData();
            checkData(shuffleDataResult, expectedBlockIdData);
            Bytes.concat(actualDataBuf, shuffleDataResult.getData());
          }
          assertNull(handler.readShuffleData());
          assertArrayEquals(expectedDataBuf, actualDataBuf);
        } else {
          assertNull(handler.readShuffleData());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testHugeData() {
    try {
      String basePath = HDFS_URI + "testHugeData";
      int readBufferSize = 7;
      Path testPath = new Path(basePath + "/app1/0/test");
      fs.mkdirs(testPath);
      Path dataPath = new Path(basePath + "/app1/0/test/1-3.data");
      HdfsFileWriter writer = new HdfsFileWriter(dataPath, conf);
      // write data and record meta data
      Map<Integer, Integer> partitionSliceNum = Maps.newHashMap();
      Map<Long, byte[]> expectedBlockIdData = Maps.newHashMap();
      Map<Integer, List<ShufflePartitionedBlock>> expectedBlocks = Maps.newHashMap();
      Map<Integer, List<FileBasedShuffleSegment>> expectedIndexSegments = Maps.newHashMap();
      int partitionNum = 3;
      int dataFileLen = 0;
      int totalIndexSegmentNum = 0;
      List<ShuffleIndexHeader.Entry> indexHeaderEntries = Lists.newArrayList();
      ByteBuffer headerContentBuf = ByteBuffer.allocate(
          (int)ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum) - ShuffleStorageUtils.getHeaderCrcLen());
      headerContentBuf.putInt(partitionNum);
      for (int partitionId = 1; partitionId <= 3; partitionId++) {
        int expectTotalBlockNum = 1024 * (new Random().nextInt(3) + 1);
        int blockSize = 1024 * (new Random().nextInt(7) + 11);
        writeTestData(writer, partitionId, expectTotalBlockNum, blockSize, 0,
            expectedBlockIdData, expectedBlocks, expectedIndexSegments, true);
        int sliceNum = calcExpectedSegmentNum(expectTotalBlockNum, blockSize, readBufferSize);
        partitionSliceNum.put(partitionId, sliceNum);
        List<FileBasedShuffleSegment> segments = expectedIndexSegments.get(partitionId);
        totalIndexSegmentNum += segments.size();
        long indexLen = segments.size() * FileBasedShuffleSegment.SEGMENT_SIZE;
        long dataLen = expectTotalBlockNum * blockSize;
        headerContentBuf.putInt(partitionId);
        headerContentBuf.putLong(indexLen);
        headerContentBuf.putLong(dataLen);
        dataFileLen += dataLen;
        indexHeaderEntries.add(new ShuffleIndexHeader.Entry(partitionId, indexLen, dataLen));
      }
      headerContentBuf.flip();
      long crc =  ChecksumUtils.getCrc32(headerContentBuf);
      ShuffleIndexHeader shuffleIndexHeader = new ShuffleIndexHeader(partitionNum, indexHeaderEntries, crc);
      writer.close();

      //  write index
      Path indexPath = new Path(basePath + "/app1/0/test/1-3.index");
      writer = new HdfsFileWriter(indexPath, conf);
      int indexFileLen =
          shuffleIndexHeader.getHeaderLen() + totalIndexSegmentNum * FileBasedShuffleSegment.SEGMENT_SIZE;
      ByteBuffer byteBuffer =  ByteBuffer.allocate(indexFileLen);
      byteBuffer.putInt(shuffleIndexHeader.getPartitionNum());
      for (ShuffleIndexHeader.Entry entry : shuffleIndexHeader.getIndexes()) {
        byteBuffer.putInt(entry.getPartitionId());
        byteBuffer.putLong(entry.getPartitionIndexLength());
        byteBuffer.putLong(entry.getPartitionDataLength());
      }
      byteBuffer.putLong(crc);
      for (int partitionId = 1; partitionId <= 3; partitionId++) {
        List<FileBasedShuffleSegment> fileBasedShuffleSegments = expectedIndexSegments.get(partitionId);
        for (FileBasedShuffleSegment segment : fileBasedShuffleSegments) {
          byteBuffer.putLong(segment.getOffset());
          byteBuffer.putInt(segment.getLength());
          byteBuffer.putInt(segment.getUncompressLength());
          byteBuffer.putLong(segment.getCrc());
          byteBuffer.putLong(segment.getBlockId());
          byteBuffer.putLong(segment.getTaskAttemptId());
        }
      }
      byteBuffer.flip();
      writer.writeData(byteBuffer);
      writer.close();

      for (int partitionId = 1; partitionId <= 3; partitionId++) {
        String fileNamePrefix = basePath + "/app1/0/test/1-3";
        Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
        Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
        HdfsShuffleReadHandler handler = new UploadedStorageHdfsShuffleReadHandler(
          "app1", 0, partitionId, fileNamePrefix, readBufferSize,
            expectBlockIds, processBlockIds, conf);

        int sliceNum = partitionSliceNum.get(partitionId);
        byte[] expectedDataBuf = new byte[0];
        for (ShufflePartitionedBlock spb : expectedBlocks.get(partitionId)) {
          Bytes.concat(expectedDataBuf, spb.getData());
          expectBlockIds.addLong(spb.getBlockId());
        }

        if (partitionId <= 3) {
          byte[] actualDataBuf = new byte[0];
          for (int i = 0; i < sliceNum; ++i) {
            ShuffleDataResult shuffleDataResult = handler.readShuffleData();
            checkData(shuffleDataResult, expectedBlockIdData);
            Bytes.concat(actualDataBuf, shuffleDataResult.getData());
          }
          assertNull(handler.readShuffleData());
          assertArrayEquals(expectedDataBuf, actualDataBuf);
        } else {
          assertNull(handler.readShuffleData());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
