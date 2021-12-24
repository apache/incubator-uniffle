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
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

/**
 * HdfsShuffleFileReadHandler is a shuffle-specific file read handler, it contains two HdfsFileReader
 * instances created by using the index file and its indexed data file.
 */
public class HdfsShuffleReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleReadHandler.class);

  protected final String filePrefix;
  protected final HdfsFileReader indexReader;
  protected final HdfsFileReader dataReader;
  protected final int readBufferSize;

  protected final List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  protected int segmentIndex;

  public HdfsShuffleReadHandler(
      String filePrefix,
      int readBufferSize,
      Configuration conf) throws IOException {
    this.filePrefix = filePrefix;
    this.readBufferSize = readBufferSize;
    this.indexReader = createHdfsReader(ShuffleStorageUtils.generateIndexFileName(filePrefix), conf);
    this.dataReader = createHdfsReader(ShuffleStorageUtils.generateDataFileName(filePrefix), conf);
  }

  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      List<ShuffleDataSegment> cur = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);
      shuffleDataSegments.addAll(cur);
    }

    if (segmentIndex >= shuffleDataSegments.size()) {
      return null;
    }
    ShuffleDataSegment shuffleDataSegment = shuffleDataSegments.get(segmentIndex++);

    // Here we make an assumption that the rest of the file is corrupted, if an unexpected data is read.
    int expectedLength = shuffleDataSegment.getLength();
    if (expectedLength <= 0) {
      LOG.warn("Invalid data segment is {} from file {}.data", shuffleDataSegment, filePrefix);
      return null;
    }

    byte[] data = readShuffleData(shuffleDataSegment.getOffset(), expectedLength);
    if (data.length == 0) {
      LOG.warn("Fail to read expected[{}] data, actual[{}] and segment is {} from file {}.data",
          expectedLength, data.length, shuffleDataSegment, filePrefix);
      return null;
    }

    ShuffleDataResult shuffleDataResult = new ShuffleDataResult(data, shuffleDataSegment.getBufferSegments());
    if (shuffleDataResult.isEmpty()) {
      LOG.warn("Shuffle data is empty, expected length {}, data length {}, segment {} in file {}.data",
          expectedLength, data.length, shuffleDataSegment, filePrefix);
      return null;
    }

    return shuffleDataResult;
  }

  protected byte[] readShuffleData(long offset, int expectedLength) {
    byte[] data = dataReader.read(offset, expectedLength);
    if (data.length != expectedLength) {
      LOG.warn("Fail to read expected[{}] data, actual[{}] from file {}.data",
          expectedLength, data.length, filePrefix);
      return new byte[0];
    }
    return data;
  }

  protected ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      byte[] indexData = indexReader.read();
      LOG.info("Read index files {}.index for {} ms", filePrefix, System.currentTimeMillis() - start);
      return new ShuffleIndexResult(indexData);
    } catch (Exception e) {
      LOG.info("Fail to read index files {}.index", filePrefix);
    }
    return new ShuffleIndexResult();
  }

  public synchronized void close() {
    try {
      dataReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close index filer reader for " + filePrefix + ".data";
      LOG.warn(message, ioe);
    }

    try {
      indexReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close data file reader for " + filePrefix + ".index";
      LOG.warn(message, ioe);
    }
  }

  protected HdfsFileReader createHdfsReader(
      String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(fileName);
    return new HdfsFileReader(path, hadoopConf);
  }

  public List<ShuffleDataSegment> getShuffleDataSegments() {
    return shuffleDataSegments;
  }

  public String getFilePrefix() {
    return filePrefix;
  }
}
