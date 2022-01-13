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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.ShuffleIndexResult;

public class UploadedStorageHdfsShuffleReadHandler extends HdfsShuffleReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedStorageHdfsShuffleReadHandler.class);

  private final int partitionId;
  private long dataFileOffset;

  public UploadedStorageHdfsShuffleReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      String filePrefix,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      Configuration conf)  throws IOException {
    super(appId, shuffleId, partitionId, filePrefix, readBufferSize, expectBlockIds, processBlockIds, conf);
    this.partitionId = partitionId;
  }

  @Override
  protected byte[] readShuffleData(long offset, int expectedLength) {
    byte[] data = dataReader.read(dataFileOffset + offset, expectedLength);
    if (data.length != expectedLength) {
      LOG.warn("Fail to read expected[{}] data, actual[{}] from file {}.data",
          expectedLength, data.length, filePrefix);
      return new byte[0];
    }
    return data;
  }

  @Override
  protected ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      byte[] indexData = indexReader.read();

      ByteBuffer byteBuffer = ByteBuffer.wrap(indexData);
      ShuffleIndexHeader shuffleIndexHeader = ShuffleIndexHeader.extractHeader(byteBuffer);
      if (shuffleIndexHeader == null) {
        LOG.error("Fail to read index from {}.index", filePrefix);
        return new ShuffleIndexResult();
      }

      int indexFileOffset = shuffleIndexHeader.getHeaderLen();
      int indexPartitionLen = 0;
      long dataFileOffset = 0;
      for (ShuffleIndexHeader.Entry entry : shuffleIndexHeader.getIndexes()) {
        int partitionId = entry.getPartitionId();
        indexPartitionLen = (int) entry.getPartitionIndexLength();
        if (partitionId != this.partitionId) {
          indexFileOffset += entry.getPartitionIndexLength();
          dataFileOffset += entry.getPartitionDataLength();
          continue;
        }

        if ((indexFileOffset + indexPartitionLen) > indexData.length) {
          LOG.error("Index of partition {} is invalid, offset = {}, length = {} in {}.index",
              partitionId, indexFileOffset, indexPartitionLen, filePrefix);
        }

        LOG.info("Read index files {}.index for {} ms", filePrefix, System.currentTimeMillis() - start);
        this.dataFileOffset = dataFileOffset;
        return new ShuffleIndexResult(
            Arrays.copyOfRange(indexData, indexFileOffset, indexFileOffset + indexPartitionLen));
      }
    } catch (Exception e) {
      LOG.info("Fail to read index files {}.index", filePrefix);
    }
    return new ShuffleIndexResult();
  }
}
