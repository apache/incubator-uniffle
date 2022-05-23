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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class HdfsShuffleUploadHandlerTest extends HdfsTestBase {

  @Test
  public void initTest() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "test_base";
    Path path = new Path(basePath);
    new HdfsShuffleUploadHandler(basePath, conf, "test", 4096, true);
    assertTrue(fs.isDirectory(path));
  }

  @Test
  public void uploadTestCombine() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "test_base";
    HdfsShuffleUploadHandler handler = new HdfsShuffleUploadHandler(
        basePath, conf, "uploadTestCombine", 4096, true);

    File dataFile1 = File.createTempFile("uploadTestCombine1", ".data", baseDir);
    File dataFile2 = File.createTempFile("uploadTestCombine2", ".data", baseDir);

    byte[] data1 = new byte[10];
    new Random().nextBytes(data1);
    try (OutputStream out = new FileOutputStream(dataFile1)) {
      out.write(data1);
    }

    byte[] data2 = new byte[30];
    new Random().nextBytes(data2);
    try (OutputStream out = new FileOutputStream(dataFile2)) {
      out.write(data2);
    }

    File indexFile1 = File.createTempFile("uploadTestCombine1", ".index", baseDir);
    File indexFile2 = File.createTempFile("uploadTestCombine2", ".index", baseDir);

    writeAndAssertResult(handler, dataFile1, dataFile2, indexFile1, indexFile2);


    String id = handler.getHdfsFilePrefixBase().split("-")[1];
    String ts = handler.getHdfsFilePrefixBase().split("-")[2];
    Path dataPath = new Path(
        basePath,"combine/uploadTestCombine-" + id + "-" + ts + "-1.data");
    Path indexPath = new Path(
        basePath,"combine/uploadTestCombine-" + id + "-" + ts + "-1.index");
    assertTrue(fs.isFile(dataPath));
    assertTrue(fs.isFile(indexPath));

    FileStatus dataFileStatus = fs.getFileStatus(dataPath);
    FileStatus indexFileStatus = fs.getFileStatus(indexPath);

    // check data file and index file length
    assertEquals(40, dataFileStatus.getLen());
    assertEquals(ShuffleStorageUtils.getIndexFileHeaderLen(2) + 20, indexFileStatus.getLen());

    // check index file header
    try (FSDataInputStream indexStream = fs.open(indexFileStatus.getPath())) {
      assertEquals(2, indexStream.readInt());
      assertEquals(1, indexStream.readInt());
      assertEquals(5L, indexStream.readLong());
      assertEquals(10L, indexStream.readLong());
      assertEquals(2, indexStream.readInt());
      assertEquals(15L, indexStream.readLong());
      assertEquals(30L, indexStream.readLong());
    }

    // check data file content
    try (FSDataInputStream dataStream = fs.open(dataFileStatus.getPath())) {
      for (int i = 0; i < data1.length; ++i) {
        assertEquals(data1[i], dataStream.readByte());
      }

      for (int i = 0; i < data2.length; ++i) {
        assertEquals(data2[i], dataStream.readByte());
      }

      assertThrows(EOFException.class, dataStream::readByte);
    }
  }

  private void writeAndAssertResult(
      HdfsShuffleUploadHandler handler,
      File dataFile1,
      File dataFile2,
      File indexFile1,
      File indexFile2) throws IOException {
    writeData(indexFile1, 5);
    writeData(indexFile2, 15);
    uploadAndVerify(
        handler,
        Lists.newArrayList(dataFile1, dataFile2),
        Lists.newArrayList(indexFile1, indexFile2),
        Lists.newArrayList(1, 2),
        40L,
        Lists.newArrayList(1, 2));
  }

  @Test
  public void uploadTestOneByOne() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "test_base";
    HdfsShuffleUploadHandler handler =
        new HdfsShuffleUploadHandler(basePath, conf, "uploadTestOneByOne", 4096, false);
    File dataFile1 = File.createTempFile("uploadTestOneByOne1", ".data", baseDir);
    File dataFile2 = File.createTempFile("uploadTestOneByOne2", ".data", baseDir);

    byte[] data1 = new byte[10];
    new Random().nextBytes(data1);
    try (OutputStream out = new FileOutputStream(dataFile1)) {
      out.write(data1);
    }

    byte[] data2 = new byte[30];
    new Random().nextBytes(data2);
    try (OutputStream out = new FileOutputStream(dataFile2)) {
      out.write(data2);
    }

    File indexFile1 = File.createTempFile("uploadTestOneByOne1", ".index", baseDir);
    File indexFile2 = File.createTempFile("uploadTestOneByOne2", ".index", baseDir);

    writeAndAssertResult(handler, dataFile1, dataFile2, indexFile1, indexFile2);

    String id = handler.getHdfsFilePrefixBase().split("-")[1];
    String ts = handler.getHdfsFilePrefixBase().split("-")[2];
    Path dataPath1 = new Path(basePath,"1/uploadTestOneByOne-" + id + "-" + ts + "-1.data");
    Path indexPath1 = new Path(basePath,"1/uploadTestOneByOne-" + id + "-" + ts + "-1.index");
    Path dataPath2 = new Path(basePath,"2/uploadTestOneByOne-" + id + "-" + ts + "-2.data");
    Path indexPath2 = new Path(basePath,"2/uploadTestOneByOne-" + id + "-" + ts + "-2.index");

    assertTrue(fs.isFile(dataPath1));
    assertTrue(fs.isFile(indexPath1));
    assertTrue(fs.isFile(dataPath2));
    assertTrue(fs.isFile(indexPath2));

    FileStatus dataFileStatus1 = fs.getFileStatus(dataPath1);
    FileStatus indexFileStatus1 = fs.getFileStatus(indexPath1);
    FileStatus dataFileStatus2 = fs.getFileStatus(dataPath2);
    FileStatus indexFileStatus2 = fs.getFileStatus(indexPath2);

    // check data file and index file length
    assertEquals(10, dataFileStatus1.getLen());
    assertEquals(ShuffleStorageUtils.getIndexFileHeaderLen(1) + 5, indexFileStatus1.getLen());
    assertEquals(30, dataFileStatus2.getLen());
    assertEquals(ShuffleStorageUtils.getIndexFileHeaderLen(1) + 15, indexFileStatus2.getLen());

    // check index file header
    try (FSDataInputStream indexStream = fs.open(indexFileStatus1.getPath())) {
      assertEquals(1, indexStream.readInt());
      assertEquals(1, indexStream.readInt());
      assertEquals(5L, indexStream.readLong());
    }

    try (FSDataInputStream indexStream = fs.open(indexFileStatus2.getPath())) {
      assertEquals(1, indexStream.readInt());
      assertEquals(2, indexStream.readInt());
      assertEquals(15L, indexStream.readLong());
    }

    // check data file content
    try (FSDataInputStream dataStream = fs.open(dataFileStatus1.getPath())) {
      for (int i = 0; i < data1.length; ++i) {
        assertEquals(data1[i], dataStream.readByte());
      }
    }

    try (FSDataInputStream dataStream = fs.open(dataFileStatus2.getPath())) {
      for (int i = 0; i < data2.length; ++i) {
        assertEquals(data2[i], dataStream.readByte());
      }

      assertThrows(EOFException.class, dataStream::readByte);
    }

  }

  @Test
  public void uploadTestCombineBestEffort() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "test_base";
    HdfsShuffleUploadHandler handler =
        new HdfsShuffleUploadHandler(basePath, conf, "uploadTestCombineBestEffort", 4096, true);
    File dataFile1 = File.createTempFile("uploadTestCombineBestEffort1", ".data", baseDir);
    File dataFile2 = File.createTempFile("uploadTestCombineBestEffort2", ".data", baseDir);
    File dataFile3 = new File(dataFile1.getAbsolutePath() + "null");

    byte[] data1 = new byte[10];
    new Random().nextBytes(data1);
    try (OutputStream out = new FileOutputStream(dataFile1)) {
      out.write(data1);
    }

    byte[] data2 = new byte[30];
    new Random().nextBytes(data2);
    try (OutputStream out = new FileOutputStream(dataFile2)) {
      out.write(data2);
    }

    File indexFile1 = File.createTempFile("uploadTestCombineBestEffort1", ".index", baseDir);
    File indexFile2 = new File(indexFile1.getAbsolutePath() + "null");
    File indexFile3 = new File(indexFile1.getAbsolutePath() + "null");

    writeAndAssert2(handler, indexFile1, 5, Lists.newArrayList(dataFile1, dataFile2, dataFile3), Lists.newArrayList(indexFile1, indexFile2, indexFile3), Lists.newArrayList(1, 2, 3), 10L, Lists.newArrayList(1));

    String id = handler.getHdfsFilePrefixBase().split("-")[1];
    String ts = handler.getHdfsFilePrefixBase().split("-")[2];
    Path dataPath = new Path(basePath,"combine/uploadTestCombineBestEffort-" + id + "-" + ts + "-1.data");
    Path indexPath = new Path(basePath,"combine/uploadTestCombineBestEffort-" + id + "-" + ts + "-1.index");

    assertTrue(fs.isFile(dataPath));
    assertTrue(fs.isFile(indexPath));

    FileStatus dataFileStatus = fs.getFileStatus(dataPath);
    FileStatus indexFileStatus = fs.getFileStatus(indexPath);

    // check data file and index file length
    assertEquals(40, dataFileStatus.getLen());
    assertEquals(ShuffleStorageUtils.getIndexFileHeaderLen(2) + 5, indexFileStatus.getLen());

    // check data file content
    try (FSDataInputStream dataStream = fs.open(dataFileStatus.getPath())) {
      for (int i = 0; i < data1.length; ++i) {
        assertEquals(data1[i], dataStream.readByte());
      }

      for (int i = 0; i < data2.length; ++i) {
        assertEquals(data2[i], dataStream.readByte());
      }
    }

    // check index file header
    try (FSDataInputStream indexStream = fs.open(indexFileStatus.getPath())) {
      assertEquals(2, indexStream.readInt());
      assertEquals(1, indexStream.readInt());
      assertEquals(5L, indexStream.readLong());
      assertEquals(10L, indexStream.readLong());
      assertEquals(2, indexStream.readInt());
      assertEquals(0L, indexStream.readLong());
      assertEquals(30L, indexStream.readLong());

      indexStream.seek(ShuffleStorageUtils.getIndexFileHeaderLen(2) + 5);
      assertThrows(EOFException.class, indexStream::readByte);

    }
  }

  @Test
  public void uploadTestOneByOneBestEffort() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "test_base";
    HdfsShuffleUploadHandler handler = new HdfsShuffleUploadHandler(
        basePath, conf, "uploadTestOneByOneBestEffort", 4096, false);
    File dataFile1 = File.createTempFile("uploadTestOneByOneBestEffort1", ".data", baseDir);
    File dataFile2 = new File(dataFile1.getAbsolutePath() + "null");
    File dataFile3 = File.createTempFile("uploadTestOneByOneBestEffort3", ".data", baseDir);

    byte[] data1 = new byte[10];
    new Random().nextBytes(data1);
    try (OutputStream out = new FileOutputStream(dataFile1)) {
      out.write(data1);
    }

    byte[] data2 = new byte[30];
    new Random().nextBytes(data2);
    try (OutputStream out = new FileOutputStream(dataFile3)) {
      out.write(data2);
    }

    File indexFile1 = File.createTempFile("uploadTestOneByOneBestEffort1", ".index", baseDir);
    File indexFile2 = new File(indexFile1.getAbsolutePath() + "null");
    File indexFile3 = new File(indexFile1.getAbsolutePath() + "null");

    writeAndAssert2(handler, indexFile1, 5, Lists.newArrayList(dataFile1, dataFile2, dataFile3), Lists.newArrayList(indexFile1, indexFile2, indexFile3), Lists.newArrayList(1, 2, 3), 10L, Lists.newArrayList(1));

    String id = handler.getHdfsFilePrefixBase().split("-")[1];
    String ts = handler.getHdfsFilePrefixBase().split("-")[2];
    Path dataPath1 = new Path(basePath,"1/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-1.data");
    Path indexPath1 = new Path(basePath,"1/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-1.index");
    Path dataPath2 = new Path(basePath,"2/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-2.data");
    Path indexPath2 = new Path(basePath,"2/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-2.index");
    Path dataPath3 = new Path(basePath,"3/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-3.data");
    Path indexPath3 = new Path(basePath,"3/uploadTestOneByOneBestEffort-" + id + "-" + ts + "-3.index");

    assertTrue(fs.isFile(dataPath1));
    assertTrue(fs.isFile(indexPath1));
    assertTrue(fs.isFile(dataPath2));
    assertFalse(fs.isFile(indexPath2));
    assertTrue(fs.isFile(dataPath3));
    assertTrue(fs.isFile(indexPath3));

    FileStatus dataFileStatus1 = fs.getFileStatus(dataPath1);
    FileStatus indexFileStatus1 = fs.getFileStatus(indexPath1);
    FileStatus dataFileStatus2 = fs.getFileStatus(dataPath2);

    // check data file and index file length
    assertEquals(10, dataFileStatus1.getLen());
    assertEquals(ShuffleStorageUtils.getIndexFileHeaderLen(1) + 5, indexFileStatus1.getLen());
    assertEquals(0, fs.getFileStatus(dataPath2).getLen());
    assertEquals(30, fs.getFileStatus(dataPath3).getLen());

    // check data file content
    try (FSDataInputStream dataStream = fs.open(dataFileStatus1.getPath())) {
      for (int i = 0; i < data1.length; ++i) {
        assertEquals(data1[i], dataStream.readByte());
      }
    }

    assertEquals(0, dataFileStatus2.getLen());

    // check index file header
    try (FSDataInputStream indexStream = fs.open(indexFileStatus1.getPath())) {
      assertEquals(1, indexStream.readInt());
      assertEquals(1, indexStream.readInt());
      assertEquals(5L, indexStream.readLong());

      assertThrows(EOFException.class, () ->
          indexStream.seek(ShuffleStorageUtils.getIndexFileHeaderLen(1) + 5 + 1));

    }

  }

  private void writeAndAssert2(
      HdfsShuffleUploadHandler handler,
      File indexFile,
      int length,
      ArrayList<File> indexFiles,
      ArrayList<File> dataFiles,
      ArrayList<Integer> partitions,
      long successSize,
      ArrayList<Integer> successPartitions) throws IOException {
    writeData(indexFile, length);
    uploadAndVerify(handler, indexFiles, dataFiles, partitions, successSize, successPartitions);
  }

  private void uploadAndVerify(
      HdfsShuffleUploadHandler handler,
      ArrayList<File> indexFiles,
      ArrayList<File> dataFiles,
      ArrayList<Integer> partitions,
      long successSize,
      ArrayList<Integer> successPartitions) {
    ShuffleUploadResult ret = handler.upload(
        indexFiles,
        dataFiles,
        partitions);
    assertEquals(successSize, ret.getSize());
    assertEquals(successPartitions, ret.getPartitions());
  }

  private void writeData(File file, int length) throws IOException {
    try (OutputStream out = new FileOutputStream(file)) {
      out.write(new byte[length]);
    }
  }

  @Test
  public void writeHeaderTest() {
    try {
      String basePath = HDFS_URI + "test_base";
      HdfsShuffleUploadHandler handler = new HdfsShuffleUploadHandler(
          basePath, conf, "uploadTestOneByOneBestEffort", 4096, false);

      File indexFile1 = File.createTempFile("writeHeaderTest1", ".index", baseDir);
      File indexFile2 = File.createTempFile("writeHeaderTest2", ".index", baseDir);
      File indexFile3 = File.createTempFile("writeHeaderTest3", ".index", baseDir);

      try (OutputStream out = new FileOutputStream(indexFile1)) {
        out.write(new byte[5]);
      }

      try (OutputStream out = new FileOutputStream(indexFile2)) {
        out.write(new byte[15]);
      }

      try (OutputStream out = new FileOutputStream(indexFile3)) {
        out.write(new byte[25]);
      }

      HdfsFileWriter writer = new HdfsFileWriter(new Path(handler.getBaseHdfsPath() + "/writeHeaderTest.index"), conf);
      writer.writeHeader(
          Lists.newArrayList(1, 2, 3),
          Lists.newArrayList(indexFile1.length(), indexFile2.length(), indexFile3.length()),
          Lists.newArrayList(1L, 2L, 3L));
      writer.close();
      byte[] buf = new byte[(int)ShuffleStorageUtils.getIndexFileHeaderLen(3)- ShuffleStorageUtils.getHeaderCrcLen()];

      FSDataInputStream readStream1 = fs.open(
          new Path(handler.getBaseHdfsPath() + "/writeHeaderTest.index"));
      assertEquals(3, readStream1.readInt());
      assertEquals(1, readStream1.readInt());
      assertEquals(5, readStream1.readLong());
      assertEquals(1, readStream1.readLong());
      assertEquals(2, readStream1.readInt());
      assertEquals(15, readStream1.readLong());
      assertEquals(2, readStream1.readLong());
      assertEquals(3, readStream1.readInt());
      assertEquals(25, readStream1.readLong());
      assertEquals(3, readStream1.readLong());

      FSDataInputStream readStream2 = fs.open(
          new Path(handler.getBaseHdfsPath() + "/writeHeaderTest.index"));
      readStream2.readFully(buf);
      long crc = readStream1.readLong();
      assertEquals(ChecksumUtils.getCrc32(buf), crc);

      readStream1.close();
      readStream2.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
