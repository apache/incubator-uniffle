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

package com.tencent.rss.server.buffer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleFlushManager;

public class ShuffleBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBuffer.class);

  private final long capacity;
  private long size;
  // blocks will be added to inFlushBlockMap as <eventId, blocks> pair
  // it will be removed after flush to storage
  // the strategy ensure that shuffle is in memory or storage
  private List<ShufflePartitionedBlock> blocks;
  private Map<Long, List<ShufflePartitionedBlock>> inFlushBlockMap;

  public ShuffleBuffer(long capacity) {
    this.capacity = capacity;
    this.size = 0;
    this.blocks = new LinkedList<>();
    this.inFlushBlockMap = Maps.newConcurrentMap();
  }

  public int append(ShufflePartitionedData data) {
    int mSize = 0;

    synchronized (this) {
      for (ShufflePartitionedBlock block : data.getBlockList()) {
        blocks.add(block);
        mSize += block.getSize();
      }
      size += mSize;
    }

    return mSize;
  }

  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid) {
    if (blocks.isEmpty()) {
      return null;
    }
    // buffer will be cleared, and new list must be created for async flush
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(blocks);
    long eventId = ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement();
    final ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        eventId,
        appId,
        shuffleId,
        startPartition,
        endPartition,
        size,
        spBlocks,
        isValid,
        this);
    inFlushBlockMap.put(eventId, spBlocks);
    blocks.clear();
    size = 0;
    return event;
  }

  public List<ShufflePartitionedBlock> getBlocks() {
    return blocks;
  }

  public long getSize() {
    return size;
  }

  public boolean isFull() {
    return size > capacity;
  }

  public synchronized void clearInFlushBuffer(long eventId) {
    inFlushBlockMap.remove(eventId);
  }

  @VisibleForTesting
  public Map<Long, List<ShufflePartitionedBlock>> getInFlushBlockMap() {
    return inFlushBlockMap;
  }

  // 1. generate buffer segments and other info: if blockId exist, start with which eventId
  // 2. according to info from step 1, generate data
  // todo: if block was flushed, it's possible to get duplicated data
  public synchronized ShuffleDataResult getShuffleData(
      long lastBlockId, int readBufferSize) {
    try {
      List<BufferSegment> bufferSegments = Lists.newArrayList();
      List<ShufflePartitionedBlock> readBlocks = Lists.newArrayList();
      updateBufferSegmentsAndResultBlocks(
          lastBlockId, readBufferSize, bufferSegments, readBlocks);
      if (!bufferSegments.isEmpty()) {
        int length = calculateDataLength(bufferSegments);
        byte[] data = new byte[length];
        // copy result data
        updateShuffleData(readBlocks, data);
        return new ShuffleDataResult(data, bufferSegments);
      }
    } catch (Exception e) {
      LOG.error("Exception happened when getShuffleData in buffer", e);
    }
    return new ShuffleDataResult();
  }

  // here is the rule to read data in memory:
  // 1. read from inFlushBlockMap order by eventId asc, then from blocks
  // 2. if can't find lastBlockId, means related data may be flushed to storage, repeat step 1
  private void updateBufferSegmentsAndResultBlocks(
      long lastBlockId,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> resultBlocks) {
    long nextBlockId = lastBlockId;
    List<Long> sortedEventId = sortFlushingEventId();
    int offset = 0;
    boolean hasLastBlockId = false;
    // read from inFlushBlockMap first to make sure the order of
    // data read is according to the order of data received
    // The number of events means how many batches are in flushing status,
    // it should be less than 5, or there has some problem with storage
    if (!inFlushBlockMap.isEmpty()) {
      for (Long eventId : sortedEventId) {
        // update bufferSegments with different strategy according to lastBlockId
        if (nextBlockId == Constants.INVALID_BLOCK_ID) {
          updateSegmentsWithoutBlockId(offset, inFlushBlockMap.get(eventId), readBufferSize,
              bufferSegments, resultBlocks);
          hasLastBlockId = true;
        } else {
          hasLastBlockId = updateSegmentsWithBlockId(offset, inFlushBlockMap.get(eventId),
              readBufferSize, nextBlockId, bufferSegments, resultBlocks);
          // if last blockId is found, read from begin with next cached blocks
          if (hasLastBlockId) {
            // reset blockId to read from begin in next cached blocks
            nextBlockId = Constants.INVALID_BLOCK_ID;
          }
        }
        if (!bufferSegments.isEmpty()) {
          offset = calculateDataLength(bufferSegments);
        }
        if (offset >= readBufferSize) {
          break;
        }
      }
    }
    // try to read from cached blocks which is not in flush queue
    if (blocks.size() > 0 && offset < readBufferSize) {
      if (nextBlockId == Constants.INVALID_BLOCK_ID) {
        updateSegmentsWithoutBlockId(offset, blocks, readBufferSize, bufferSegments, resultBlocks);
        hasLastBlockId = true;
      } else {
        hasLastBlockId = updateSegmentsWithBlockId(offset, blocks,
            readBufferSize, nextBlockId, bufferSegments, resultBlocks);
      }
    }
    if ((!inFlushBlockMap.isEmpty() || blocks.size() > 0) && offset == 0 && !hasLastBlockId) {
      // can't find lastBlockId, it should be flushed
      // but there still has data in memory
      // try read again with blockId = Constants.INVALID_BLOCK_ID
      updateBufferSegmentsAndResultBlocks(
          Constants.INVALID_BLOCK_ID, readBufferSize, bufferSegments, resultBlocks);
    }
  }

  private int calculateDataLength(List<BufferSegment> bufferSegments) {
    BufferSegment bufferSegment = bufferSegments.get(bufferSegments.size() - 1);
    return bufferSegment.getOffset() + bufferSegment.getLength();
  }

  private void updateShuffleData(List<ShufflePartitionedBlock> readBlocks, byte[] data) {
    int offset = 0;
    for (ShufflePartitionedBlock block : readBlocks) {
      // fill shuffle data
      try {
        System.arraycopy(block.getData(), 0, data, offset, block.getLength());
      } catch (Exception e) {
        LOG.error("Unexpect exception for System.arraycopy, length["
            + block.getLength() + "], offset["
            + offset + "], dataLength[" + data.length + "]", e);
        throw e;
      }
      offset += block.getLength();
    }
  }

  private List<Long> sortFlushingEventId() {
    List<Long> eventIdList = Lists.newArrayList(inFlushBlockMap.keySet());
    eventIdList.sort((id1, id2) -> {
      if (id1 > id2) {
        return 1;
      }
      return -1;
    });
    return eventIdList;
  }

  private void updateSegmentsWithoutBlockId(
      int offset,
      List<ShufflePartitionedBlock> cachedBlocks,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> readBlocks) {
    int currentOffset = offset;
    // read from first block
    for (ShufflePartitionedBlock block : cachedBlocks) {
      // add bufferSegment with block
      bufferSegments.add(new BufferSegment(block.getBlockId(), currentOffset, block.getLength(),
          block.getUncompressLength(), block.getCrc(), block.getTaskAttemptId()));
      readBlocks.add(block);
      // update offset
      currentOffset += block.getLength();
      // check if length >= request buffer size
      if (currentOffset >= readBufferSize) {
        break;
      }
    }
  }

  private boolean updateSegmentsWithBlockId(
      int offset,
      List<ShufflePartitionedBlock> cachedBlocks,
      long readBufferSize,
      long lastBlockId,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> readBlocks) {
    int currentOffset = offset;
    // find lastBlockId, then read from next block
    boolean foundBlockId = false;
    for (ShufflePartitionedBlock block : cachedBlocks) {
      if (!foundBlockId) {
        // find lastBlockId
        if (block.getBlockId() == lastBlockId) {
          foundBlockId = true;
        }
        continue;
      }
      // add bufferSegment with block
      bufferSegments.add(new BufferSegment(block.getBlockId(), currentOffset, block.getLength(),
          block.getUncompressLength(), block.getCrc(), block.getTaskAttemptId()));
      readBlocks.add(block);
      // update offset
      currentOffset += block.getLength();
      if (currentOffset >= readBufferSize) {
        break;
      }
    }
    return foundBlockId;
  }
}
