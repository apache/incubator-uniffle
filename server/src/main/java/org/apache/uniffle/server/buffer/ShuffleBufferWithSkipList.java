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

package org.apache.uniffle.server.buffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleFlushManager;

public class ShuffleBufferWithSkipList extends ShuffleBuffer {
  private ConcurrentSkipListMap<Long, ShufflePartitionedBlock> blocksMap;
  private final Map<Long, ConcurrentSkipListMap<Long, ShufflePartitionedBlock>> inFlushBlockMap;

  public ShuffleBufferWithSkipList(long capacity) {
    super(capacity);
    this.blocksMap = newConcurrentSkipListMap();
    this.inFlushBlockMap = JavaUtils.newConcurrentMap();
  }

  private ConcurrentSkipListMap<Long, ShufflePartitionedBlock> newConcurrentSkipListMap() {
    // Sort by taskId and blockId.
    return new ConcurrentSkipListMap<>(Comparator.comparingLong(Long::reverse));
  }

  @Override
  public long append(ShufflePartitionedData data) {
    long mSize = 0;

    synchronized (this) {
      for (ShufflePartitionedBlock block : data.getBlockList()) {
        blocksMap.put(block.getBlockId(), block);
        mSize += block.getSize();
      }
      size += mSize;
    }

    return mSize;
  }

  @Override
  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid,
      ShuffleDataDistributionType dataDistributionType) {
    if (blocksMap.isEmpty()) {
      return null;
    }
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(blocksMap.values());
    long eventId = ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement();
    final ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(
            eventId, appId, shuffleId, startPartition, endPartition, size, spBlocks, isValid, this);
    event.addCleanupCallback(
        () -> {
          this.clearInFlushBuffer(event.getEventId());
          spBlocks.forEach(spb -> spb.getData().release());
        });
    inFlushBlockMap.put(eventId, blocksMap);
    blocksMap = newConcurrentSkipListMap();
    size = 0;
    return event;
  }

  @Override
  public List<ShufflePartitionedBlock> getBlocks() {
    return new LinkedList<>(blocksMap.values());
  }

  @Override
  public void release() {
    blocksMap.values().forEach(spb -> spb.getData().release());
  }

  @Override
  public synchronized void clearInFlushBuffer(long eventId) {
    inFlushBlockMap.remove(eventId);
  }

  @Override
  public Map<Long, List<ShufflePartitionedBlock>> getInFlushBlockMap() {
    return inFlushBlockMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue().values())));
  }

  @Override
  protected void updateBufferSegmentsAndResultBlocks(
      long lastBlockId,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> resultBlocks,
      Roaring64NavigableMap expectedTaskIds) {
    long nextBlockId = lastBlockId;
    List<Long> sortedEventId = sortFlushingEventId(inFlushBlockMap);
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
          updateSegmentsWithoutBlockId(
              offset,
              inFlushBlockMap.get(eventId).values(),
              readBufferSize,
              bufferSegments,
              resultBlocks,
              expectedTaskIds);
          hasLastBlockId = true;
        } else {
          hasLastBlockId =
              updateSegmentsWithBlockId(
                  offset,
                  inFlushBlockMap.get(eventId),
                  readBufferSize,
                  nextBlockId,
                  bufferSegments,
                  resultBlocks,
                  expectedTaskIds);
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
    if (!blocksMap.isEmpty() && offset < readBufferSize) {
      if (nextBlockId == Constants.INVALID_BLOCK_ID) {
        updateSegmentsWithoutBlockId(
            offset,
            blocksMap.values(),
            readBufferSize,
            bufferSegments,
            resultBlocks,
            expectedTaskIds);
        hasLastBlockId = true;
      } else {
        hasLastBlockId =
            updateSegmentsWithBlockId(
                offset,
                blocksMap,
                readBufferSize,
                nextBlockId,
                bufferSegments,
                resultBlocks,
                expectedTaskIds);
      }
    }
    if ((!inFlushBlockMap.isEmpty() || !blocksMap.isEmpty()) && offset == 0 && !hasLastBlockId) {
      // can't find lastBlockId, it should be flushed
      // but there still has data in memory
      // try read again with blockId = Constants.INVALID_BLOCK_ID
      updateBufferSegmentsAndResultBlocks(
          Constants.INVALID_BLOCK_ID,
          readBufferSize,
          bufferSegments,
          resultBlocks,
          expectedTaskIds);
    }
  }

  private boolean updateSegmentsWithBlockId(
      int offset,
      ConcurrentSkipListMap<Long, ShufflePartitionedBlock> cachedBlocks,
      long readBufferSize,
      long lastBlockId,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> readBlocks,
      Roaring64NavigableMap expectedTaskIds) {
    int currentOffset = offset;
    // find lastBlockId, then read from next block
    if (cachedBlocks.get(lastBlockId) == null) {
      return false;
    }
    ConcurrentNavigableMap<Long, ShufflePartitionedBlock> remainingBlocks =
        cachedBlocks.tailMap(lastBlockId);
    for (ShufflePartitionedBlock block : remainingBlocks.values()) {
      if (expectedTaskIds != null && !expectedTaskIds.contains(block.getTaskAttemptId())) {
        continue;
      }
      // add bufferSegment with block
      bufferSegments.add(
          new BufferSegment(
              block.getBlockId(),
              currentOffset,
              block.getLength(),
              block.getUncompressLength(),
              block.getCrc(),
              block.getTaskAttemptId()));
      readBlocks.add(block);
      // update offset
      currentOffset += block.getLength();
      if (currentOffset >= readBufferSize) {
        break;
      }
    }
    return true;
  }
}
