/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.storage.handler.impl;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.annotations.VisibleForTesting;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.segment.SegmentSplitter;
import org.apache.uniffle.common.segment.SegmentSplitterFactory;

public abstract class DataSkippableReadHandler extends AbstractClientReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkippableReadHandler.class);

  private volatile int segmentSize = 0;
  private volatile ConcurrentLinkedQueue<ShuffleDataSegment> shuffleDataSegmentsQueue;

  protected Roaring64NavigableMap expectBlockIds;
  protected Roaring64NavigableMap processBlockIds;

  private SegmentSplitter segmentSplitter;

  @VisibleForTesting
  DataSkippableReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      SegmentSplitter segmentSplitter) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
    this.segmentSplitter = segmentSplitter;
  }

  public DataSkippableReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      ShuffleDataDistributionType distributionType,
      Roaring64NavigableMap expectTaskIds) {
    this(
        appId,
        shuffleId,
        partitionId,
        readBufferSize,
        expectBlockIds,
        processBlockIds,
        SegmentSplitterFactory
            .getInstance()
            .get(distributionType, expectTaskIds, readBufferSize)
    );
  }

  protected abstract ShuffleIndexResult readShuffleIndex();

  protected abstract ShuffleDataResult readShuffleData(ShuffleDataSegment segment);

  // Thread safe
  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegmentsQueue == null) {
      synchronized (this) {
        if (shuffleDataSegmentsQueue == null) {
          ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
          if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
            return null;
          }
          List<ShuffleDataSegment> shuffleDataSegments = segmentSplitter.split(shuffleIndexResult);
          segmentSize = shuffleDataSegments.size();
          final ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();
          queue.addAll(shuffleDataSegments);
          shuffleDataSegmentsQueue = queue;
        }
      }
    }

    // We should skip unexpected and processed segments when handler is read
    ShuffleDataResult result = null;
    ShuffleDataSegment segment;
    while ((segment = shuffleDataSegmentsQueue.poll()) != null) {
      Roaring64NavigableMap blocksOfSegment = Roaring64NavigableMap.bitmapOf();
      segment.getBufferSegments().forEach(block -> blocksOfSegment.addLong(block.getBlockId()));
      // skip unexpected blockIds
      blocksOfSegment.and(expectBlockIds);
      if (!blocksOfSegment.isEmpty()) {
        // skip processed blockIds
        blocksOfSegment.or(processBlockIds);
        blocksOfSegment.xor(processBlockIds);
        if (!blocksOfSegment.isEmpty()) {
          result = readShuffleData(segment);
          return result;
        }
      }
    }
    return result;
  }

  // Only for tests.
  @VisibleForTesting
  public int getShuffleDataSegmentsSize() {
    return segmentSize;
  }
}
