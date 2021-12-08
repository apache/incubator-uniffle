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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsClientReadHandler extends AbstractHdfsClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsClientReadHandler.class);

  private final List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  // TODO: Refactor read handler to wrapper a single index file and its indexed data file into a handler.
  private final Iterator<Entry<String, HdfsFileReader>> readerIterator;
  private HdfsFileReader currentIndexReader;
  private HdfsFileReader currentDataReader;

  public HdfsClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Configuration hadoopConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    this.hadoopConf = hadoopConf;
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(appId,
            shuffleId, partitionId, partitionNumPerRange, partitionNum));
    init(fullShufflePath);
    readerIterator = indexReaderMap
        .entrySet()
        .stream()
        .sorted(Entry.comparingByKey())
        .collect(Collectors.toList())
        .iterator();
  }

  public ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      byte[] indexData = currentIndexReader.readIndex();
      LOG.info("Read index files for shuffleId[{}], partitionId[{}] for {} ms",
          shuffleId, partitionId, System.currentTimeMillis() - start);
      return new ShuffleIndexResult(indexData);
    } catch (Exception e) {
      LOG.info("Fail to read index files for shuffleId[{}], partitionId[{}]", shuffleId, partitionId);
    }

    return new ShuffleIndexResult();
  }

  public ShuffleDataResult readShuffleData(ShuffleDataSegment shuffleDataSegment) {
    int expectedLength = shuffleDataSegment.getLength();
    if (expectedLength < 0) {
      LOG.error("Invalid segment {}", shuffleDataSegment);
      return null;
    }

    byte[] data = currentDataReader.readData(shuffleDataSegment.getOffset(), expectedLength);
    if (data.length != expectedLength) {
      LOG.error("Fail to read expected[{}] data, actual[{}] and segment is {}",
          expectedLength, data.length, shuffleDataSegment);
      return null;
    }

    return new ShuffleDataResult(data, shuffleDataSegment.getBufferSegments());
  }

  @Override
  public ShuffleDataResult readShuffleData(int segmentIndex) {
    if (segmentIndex >= shuffleDataSegments.size()) {
      ShuffleIndexResult shuffleIndexResult = null;
      while (readerIterator.hasNext()) {
        Entry<String, HdfsFileReader> entry = readerIterator.next();
        String key = entry.getKey();
        currentIndexReader = entry.getValue();
        if (currentIndexReader == null) {
          LOG.error("Index reader of {} is invalid!", key);
          continue;
        }

        currentDataReader = dataReaderMap.get(entry.getKey());
        if (currentDataReader == null) {
          LOG.error("Data reader of {} is invalid!", key);
          continue;
        }

        shuffleIndexResult = readShuffleIndex();
        if (!shuffleIndexResult.isEmpty()) {
          break;
        }
      }

      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      List<ShuffleDataSegment> cur = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);
      shuffleDataSegments.addAll(cur);
    }

    if (segmentIndex >= shuffleDataSegments.size()) {
      return null;
    }

    return readShuffleData(shuffleDataSegments.get(segmentIndex));
  }

  @VisibleForTesting
  List<ShuffleDataSegment> getShuffleDataSegments() {
    return shuffleDataSegments;
  }

  @VisibleForTesting
  public Iterator<Entry<String, HdfsFileReader>> getReaderIterator() {
    return readerIterator;
  }
}
