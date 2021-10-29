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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HdfsFileReaderTest extends HdfsTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createStreamTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamTest");
    fs.create(path);

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(0L, reader.getOffset());
    }

    fs.deleteOnExit(path);
  }

  @Test
  public void createStreamAppendTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamFirstTest");

    assertFalse(fs.isFile(path));
    try {
      new HdfsFileReader(path, conf);
      fail("Exception should be thrown");
    } catch (IllegalStateException ise) {
      ise.getMessage().startsWith(HDFS_URI + "createStreamFirstTest don't exist");
    }
  }

  @Test
  public void readDataTest() throws IOException {
    Path path = new Path(HDFS_URI, "readDataTest");
    byte[] data = new byte[160];
    int offset = 128;
    int length = 32;
    new Random().nextBytes(data);
    long crc11 = ChecksumUtils.getCrc32(ByteBuffer.wrap(data, offset, length));

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      writer.writeData(data);
    }
    FileBasedShuffleSegment segment = new FileBasedShuffleSegment(23, offset, length, length, 0xdeadbeef, 1);
    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      byte[] actual = reader.readData(segment.getOffset(), segment.getLength());
      long crc22 = ChecksumUtils.getCrc32(actual);

      for (int i = 0; i < length; ++i) {
        assertEquals(data[i + offset], actual[i]);
      }
      assertEquals(crc11, crc22);
      // EOF exception is expected
      segment = new FileBasedShuffleSegment(23, offset * 2, length, length, 1, 1);
      assertNull(reader.readData(segment.getOffset(), segment.getLength()));
    }
  }

  @Test
  public void readIndexTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(123, 0, 32, 32, 1, 1),
        new FileBasedShuffleSegment(223, 32, 23, 23, 2, 1),
        new FileBasedShuffleSegment(323, 64, 32, 32, 3, 2)
    };

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }
    }

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      // test limit
      int limit = 2;
      List<FileBasedShuffleSegment> idx = reader.readIndex(limit);
      assertEquals(2, idx.size());

      for (int i = 0; i < limit; ++i) {
        assertEquals(segments[i], idx.get(i));
      }

      long expected = 2 * (4 * 8 + 2 * 4); // segment length = 4 * 8 + 2 * 4
      assertEquals(expected, reader.getOffset());

      idx = reader.readIndex(1000);
      assertEquals(1, idx.size());
      assertEquals(segments[2], idx.get(0));

      expected = 3 * (4 * 8 + 2 * 4);
      assertEquals(expected, reader.getOffset());
    }
  }

  @Test
  public void readIndexFailTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexFailTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(123, 0, 32, 32, 1, 1),
        new FileBasedShuffleSegment(223, 32, 23, 32, 2, 1),
        new FileBasedShuffleSegment(323, 64, 32, 32, 3, 2)
    };

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }

      int[] data = {1, 3, 5, 7, 9};

      ByteBuffer buf = ByteBuffer.allocate(4 * data.length);
      buf.asIntBuffer().put(data);
      writer.writeData(buf.array());
    }

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      // test limit
      int limit = 10;
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Invalid index file");
      reader.readIndex(limit);
    }
  }

  @Test
  public void readUploadFileHeaderTest() {
    try {
      Path path = new Path(HDFS_URI, "readUploadFileHeaderTest");
      List<Integer> partitionList = Lists.newArrayList(1, 2, 3);
      List<Long> sizeList = Lists.newArrayList(1L, 2L, 3L);
      List<Long> fileSizeList = Lists.newArrayList(1L, 2L, 3L);
      try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
        writer.writeHeader(partitionList, sizeList, fileSizeList);
      }
      try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
        ShuffleIndexHeader header = reader.readHeader();
        assertEquals(partitionList.size(), header.getPartitionNum());
        assertEquals(partitionList.size(), header.getIndexes().size());
        for (int i = 0; i < header.getPartitionNum(); i++) {
          ShuffleIndexHeader.Entry entry = header.getIndexes().get(i);
          assertEquals(partitionList.get(i), entry.getPartitionId());
          assertEquals(sizeList.get(i), entry.getPartitionIndexLength());
          assertEquals(fileSizeList.get(i), entry.getPartitionDataLength());
        }
      }

      Path failPath = new Path(HDFS_URI, "readUploadFileHeaderFailTest");
      try (HdfsFileWriter writer = new HdfsFileWriter(failPath, conf)) {
        ByteBuffer headerContentBuf = ByteBuffer.allocate(4 + 4 + 8 + 4 + 4);
        headerContentBuf.putInt(1);
        headerContentBuf.putInt(1);
        headerContentBuf.putLong(1);
        headerContentBuf.putLong(1);
        headerContentBuf.flip();
        writer.writeData(headerContentBuf);
      }
      try (HdfsFileReader reader = new HdfsFileReader(failPath, conf)) {
        boolean isException = false;
        try {
          ShuffleIndexHeader header = reader.readHeader();
        } catch (IOException ioe) {
          isException = true;
        }
        assertTrue(isException);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void bigIndexDataTest() {
    try {
      String basePath = HDFS_URI + "test_big_data/";
      HdfsFileWriter writer = new HdfsFileWriter(new Path(basePath + "1.index"), conf);
      List<Integer> indexes = Lists.newArrayList();
      List<Long> sizes = Lists.newArrayList();
      List<Long> fileSizes = Lists.newArrayList();
      for (int i = 0; i < 1024; i++) {
        indexes.add(i);
        sizes.add(10240L);
        fileSizes.add(1024L);
      }
      writer.writeHeader(indexes, sizes, fileSizes);
      long z = 0;
      for (int i = 0; i < 1024; i++) {
        for (int j = 0; j < 1024; j++) {
          FileBasedShuffleSegment segment = new FileBasedShuffleSegment(z, 1L, 1, 1, 1L, 1L);
          writer.writeIndex(segment);
          z++;
        }
      }
      writer.close();
      HdfsFileReader fileReader = new HdfsFileReader(new Path(basePath + "1.index"), conf);
      ShuffleIndexHeader header = fileReader.readHeader();
      sizes = Lists.newArrayList();
      List<Integer> partitions = Lists.newArrayList();
      for (ShuffleIndexHeader.Entry entry : header.getIndexes()) {
        partitions.add(entry.getPartitionId());
        sizes.add(entry.getPartitionIndexLength());
      }
      long totalSize = header.getHeaderLen();
      long count = 0;
      for (long size : sizes) {
        long actualSize = fileReader.readIndex((int) size).size();
        totalSize = totalSize + actualSize * FileBasedShuffleSegment.SEGMENT_SIZE;
        count++;
      }
      fileReader.close();
      assertEquals(sizes.size(), count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }}
