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

import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HdfsFileWriterTest extends HdfsTestBase {

  @Test
  public void createStreamFirstTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamFirstTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(0, writer.nextOffset());
    }
  }

  @Test
  public void createStreamAppendTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);

    // create a file and fill 32 bytes
    Path path = new Path(HDFS_URI, "createStreamAppendTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(data);
      assertEquals(32, writer.nextOffset());
    }

    // open existing file using append
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(32, writer.nextOffset());
    }

    // disable the append support
    conf.setBoolean("dfs.support.append", false);
    assertTrue(fs.isFile(path));
    Throwable ise = assertThrows(IllegalStateException.class, () -> new HdfsFileWriter(path, conf));
    assertTrue(ise.getMessage().startsWith(path + " exists but append mode is not support!"));
  }

  @Test
  public void createStreamDirectory() throws IOException {
    // create a file and fill 32 bytes
    Path path = new Path(HDFS_URI, "createStreamDirectory");
    fs.mkdirs(path);

    Throwable ise = assertThrows(IllegalStateException.class, () -> new HdfsFileWriter(path, conf));
    assertTrue(ise.getMessage().startsWith(HDFS_URI + "createStreamDirectory is a directory!"));
  }

  @Test
  public void createStreamTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);
    ByteBuffer buf = ByteBuffer.allocate(32);
    buf.put(data);
    Path path = new Path(HDFS_URI, "createStreamTest");

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      buf.flip();
      writer.writeData(buf.array());
      assertEquals(32, writer.nextOffset());
    }
  }

  @Test
  public void writeBufferTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);

    Path path = new Path(HDFS_URI, "writeBufferTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(data);
      assertEquals(32, writer.nextOffset());
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      for (int i = 0; i < data.length; ++i) {
        assertEquals(data[i], in.readByte());
      }
      // EOF exception is expected
      assertThrows(EOFException.class, in::readInt);
    }

  }

  @Test
  public void writeBufferArrayTest() throws IOException {
    int[] data = {1, 3, 5, 7, 9};

    ByteBuffer buf = ByteBuffer.allocate(4 * data.length);
    buf.asIntBuffer().put(data);

    Path path = new Path(HDFS_URI, "writeBufferArrayTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(buf.array());
      assertEquals(20, writer.nextOffset());
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      for (int i = 0; i < data.length; ++i) {
        assertEquals(data[i], in.readInt());
      }
      // EOF exception is expected
      assertThrows(EOFException.class, in::readInt);
    }
  }

  @Test
  public void writeSegmentTest() throws IOException {
    FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
        23, 128, 32, 32, 0xdeadbeef, 0);

    Path path = new Path(HDFS_URI, "writeSegmentTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      writer.writeIndex(segment);
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      assertEquals(128, in.readLong());
      assertEquals(32, in.readInt());
      assertEquals(32, in.readInt());
      assertEquals(0xdeadbeef, in.readLong());
      assertEquals(23, in.readLong());
    }
  }
}
