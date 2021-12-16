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

import com.google.common.collect.Lists;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleIndexHeader {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleIndexHeader.class);

  private int partitionNum;
  private List<Entry> indexes = Lists.newArrayList();
  private long crc;

  public ShuffleIndexHeader(int partitionNum, List<Entry> indexes, long crc) {
    this.partitionNum = partitionNum;
    this.indexes = indexes;
    this.crc = crc;
  }

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public List<Entry> getIndexes() {
    return indexes;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public int getHeaderLen() {
    return (int)ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum);
  }

  // No side effects on byteBuffer
  public static ShuffleIndexHeader extractHeader(ByteBuffer byteBuffer) {
    try {
      int partitionNum = byteBuffer.getInt();
      ByteBuffer headerContentBuf = ByteBuffer.allocate(
          (int) ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum)
              - ShuffleStorageUtils.getHeaderCrcLen());
      headerContentBuf.putInt(partitionNum);
      List<Entry> entries = Lists.newArrayList();

      for (int i = 0; i < partitionNum; i++) {
        int partitionId = byteBuffer.getInt();
        long partitionLength = byteBuffer.getLong();
        long partitionDataFileLength = byteBuffer.getLong();
        headerContentBuf.putInt(partitionId);
        headerContentBuf.putLong(partitionLength);
        headerContentBuf.putLong(partitionDataFileLength);

        ShuffleIndexHeader.Entry entry
            = new ShuffleIndexHeader.Entry(partitionId, partitionLength, partitionDataFileLength);
        entries.add(entry);
      }

      headerContentBuf.flip();
      long crc = byteBuffer.getLong();
      long actualCrc = ChecksumUtils.getCrc32(headerContentBuf);
      if (crc != actualCrc) {
        LOG.error("Read header exception, expected crc[{}] != actual crc[{}]", crc, actualCrc);
        return null;
      }
      // clear the side effect on byteBuffer
      byteBuffer.clear();
      return new ShuffleIndexHeader(partitionNum, entries, crc);
    } catch (Exception e) {
      LOG.error("Fail to extract header from {}, with exception", byteBuffer.toString(), e);
      return null;
    }
  }

  static class Entry {
    int partitionId;
    long partitionIndexLength;
    long partitionDataLength;

    Entry(int partitionId, long partitionIndexLength, long partitionDataLength) {
      this.partitionId = partitionId;
      this.partitionIndexLength = partitionIndexLength;
      this.partitionDataLength = partitionDataLength;
    }

    public int getPartitionId() {
      return partitionId;
    }

    public long getPartitionIndexLength() {
      return partitionIndexLength;
    }

    public long getPartitionDataLength() {
      return partitionDataLength;
    }
  }
}
