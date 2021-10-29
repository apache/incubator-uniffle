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

import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileReader implements ShuffleReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);
  private Path path;
  private Configuration hadoopConf;
  private FSDataInputStream fsDataInputStream;

  public HdfsFileReader(Path path, Configuration hadoopConf) throws IOException, IllegalStateException {
    this.path = path;
    this.hadoopConf = hadoopConf;
    createStream();
  }

  private void createStream() throws IOException, IllegalStateException {
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);

    if (!fileSystem.isFile(path)) {
      String msg = path + " don't exist or is not a file.";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    fsDataInputStream = fileSystem.open(path);
  }

  public byte[] readData(long offset, int length) {
    try {
      fsDataInputStream.seek(offset);
      byte[] buf = new byte[length];
      fsDataInputStream.readFully(buf);
      return buf;
    } catch (Exception e) {
      LOG.warn("Can't read data for path:" + path + " with offset["
          + offset + "], length[" + length + "]", e);
    }
    return null;
  }

  public void seek(long offset) throws Exception {
    fsDataInputStream.seek(offset);
  }

  public List<FileBasedShuffleSegment> readIndex(int limit) throws IOException, IllegalStateException {
    List<FileBasedShuffleSegment> ret = new LinkedList<>();

    for (int i = 0; i < limit; ++i) {
      FileBasedShuffleSegment segment = readIndex();
      if (segment == null) {
        break;
      }
      ret.add(segment);
    }

    return ret;
  }

  public FileBasedShuffleSegment readIndex() throws IOException, IllegalStateException {
    long offset;
    long pos = fsDataInputStream.getPos();
    try {
      offset = fsDataInputStream.readLong();
      int length = fsDataInputStream.readInt();
      int uncompressLength = fsDataInputStream.readInt();
      long crc = fsDataInputStream.readLong();
      long blockId = fsDataInputStream.readLong();
      long taskAttemptId = fsDataInputStream.readLong();
      return new FileBasedShuffleSegment(blockId, offset, length, uncompressLength, crc, taskAttemptId);
    } catch (Exception eof) {
      if (fsDataInputStream.getPos() != pos) {
        throw new IllegalStateException("Invalid index file " + path  + " start pos " + pos
        + " end pos " + fsDataInputStream.getPos());
      }
      return null;
    }
  }

  public ShuffleIndexHeader readHeader() throws IOException, IllegalStateException {
    ShuffleIndexHeader header = new ShuffleIndexHeader();
    header.setPartitionNum(fsDataInputStream.readInt());
    ByteBuffer headerContentBuf = ByteBuffer.allocate(
        (int)ShuffleStorageUtils.getIndexFileHeaderLen(header.getPartitionNum())
            - ShuffleStorageUtils.getHeaderCrcLen());
    headerContentBuf.putInt(header.getPartitionNum());
    for (int i = 0; i < header.getPartitionNum(); i++) {
      int partitionId = fsDataInputStream.readInt();
      long partitionLength = fsDataInputStream.readLong();
      long partitionDataFileLength = fsDataInputStream.readLong();
      headerContentBuf.putInt(partitionId);
      headerContentBuf.putLong(partitionLength);
      headerContentBuf.putLong(partitionDataFileLength);

      ShuffleIndexHeader.Entry entry
          = new ShuffleIndexHeader.Entry(partitionId, partitionLength, partitionDataFileLength);
      header.getIndexes().add(entry);
    }
    headerContentBuf.flip();
    header.setCrc(fsDataInputStream.readLong());
    long actualCrc = ChecksumUtils.getCrc32(headerContentBuf);
    if (actualCrc != header.getCrc()) {
      throw new IOException("read header exception: crc error expect: "
          + header.getCrc() + " actualCrc " + actualCrc);
    }
    return header;
  }

  public long getOffset() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataInputStream != null) {
      fsDataInputStream.close();
    }
  }

}
