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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleFlushManager;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;

public class GrpcShuffleBufferManager extends AbstractShuffleBufferManager {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcShuffleBufferManager.class);

  public GrpcShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    super(conf, shuffleFlushManager);
    long heapSize = Runtime.getRuntime().maxMemory();
    this.capacity = conf.getSizeAsBytes(ShuffleServerConf.SERVER_BUFFER_CAPACITY);
    if (this.capacity < 0) {
      this.capacity =
          (long) (heapSize * conf.getDouble(ShuffleServerConf.SERVER_BUFFER_CAPACITY_RATIO));
    }
    this.readCapacity = conf.getSizeAsBytes(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY);
    if (this.readCapacity < 0) {
      this.readCapacity =
          (long) (heapSize * conf.getDouble(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY_RATIO));
    }
    LOG.info(
        "Init shuffle buffer manager with capacity: {}, read buffer capacity: {}.",
        capacity,
        readCapacity);
    this.highWaterMark =
        (long)
            (capacity
                / 100.0
                * conf.get(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE));
    this.lowWaterMark =
        (long)
            (capacity
                / 100.0
                * conf.get(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE));
    this.hugePartitionMemoryLimitSize =
        Math.round(
            capacity * conf.get(ShuffleServerConf.HUGE_PARTITION_MEMORY_USAGE_LIMITATION_RATIO));
  }

  @Override
  public StatusCode registerBuffer(
      String appId, int shuffleId, int startPartition, int endPartition) {
    bufferPool.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, RangeMap<Integer, AbstractShuffleBuffer>> shuffleIdToBuffers =
        bufferPool.get(appId);
    shuffleIdToBuffers.computeIfAbsent(shuffleId, key -> TreeRangeMap.create());
    RangeMap<Integer, AbstractShuffleBuffer> bufferRangeMap = shuffleIdToBuffers.get(shuffleId);
    if (bufferRangeMap.get(startPartition) == null) {
      ShuffleServerMetrics.counterTotalPartitionNum.inc();
      ShuffleServerMetrics.gaugeTotalPartitionNum.inc();
      bufferRangeMap.put(Range.closed(startPartition, endPartition), new GrpcShuffleBuffer());
    } else {
      LOG.warn(
          "Already register for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], startPartition["
              + startPartition
              + "], endPartition["
              + endPartition
              + "]");
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public void appendBuffer(
      String appId,
      int shuffleId,
      boolean isPreAllocated,
      ShufflePartitionedData spd,
      int avgEstimatedSize,
      AbstractShuffleBuffer buffer) {
    long size = buffer.append(spd, avgEstimatedSize);
    if (!isPreAllocated) {
      updateUsedMemory(size);
    }
    updateShuffleSize(appId, shuffleId, size);
  }

  @Override
  protected long getBufferSize(AbstractShuffleBuffer buffer) {
    return buffer.getSize();
  }

  @Override
  protected long getActualUsedBuffer() {
    return usedMemory.get() - preAllocatedSize.get() - inFlushSize.get();
  }

  @Override
  protected void flushBuffer(
      AbstractShuffleBuffer buffer,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      boolean isHugePartition) {
    ShuffleDataFlushEvent event =
        buffer.toFlushEvent(
            appId,
            shuffleId,
            startPartition,
            endPartition,
            () -> bufferPool.containsKey(appId),
            shuffleFlushManager.getDataDistributionType(appId));
    if (event != null) {
      event.addCleanupCallback(() -> releaseMemory(event.getSize(), true, false));
      updateShuffleSize(appId, shuffleId, -event.getSize());
      inFlushSize.addAndGet(event.getSize());
      if (isHugePartition) {
        event.markOwnedByHugePartition();
      }
      ShuffleServerMetrics.gaugeInFlushBufferSize.set(inFlushSize.get());
      shuffleFlushManager.addToFlushQueue(event);
    }
  }

  @Override
  public boolean updateUsedMemory(long delta) {
    if (delta < 0) {
      if (usedMemory.get() + delta < 0) {
        return false;
      }
    }
    usedMemory.addAndGet(delta);
    ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());
    return true;
  }

  @Override
  public long getUsedMemory() {
    return usedMemory.get();
  }

  @Override
  public void removeBufferByShuffleId(String appId, Collection<Integer> shuffleIds) {
    Map<Integer, RangeMap<Integer, AbstractShuffleBuffer>> shuffleIdToBuffers =
        bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return;
    }

    Map<Integer, AtomicLong> shuffleIdToSizeMap = shuffleSizeMap.get(appId);
    for (int shuffleId : shuffleIds) {
      long size = 0;

      RangeMap<Integer, AbstractShuffleBuffer> bufferRangeMap =
          shuffleIdToBuffers.remove(shuffleId);
      if (bufferRangeMap == null) {
        continue;
      }
      Collection<AbstractShuffleBuffer> buffers = bufferRangeMap.asMapOfRanges().values();
      if (buffers != null) {
        for (AbstractShuffleBuffer buffer : buffers) {
          buffer.getBlocks().forEach(spb -> spb.getData().release());
          ShuffleServerMetrics.gaugeTotalPartitionNum.dec();
          size += buffer.getSize();
        }
      }
      releaseMemory(size, false, false);
      if (shuffleIdToSizeMap != null) {
        shuffleIdToSizeMap.remove(shuffleId);
      }
    }
  }

  @Override
  public boolean limitHugePartition(
      String appId, int shuffleId, int partitionId, long usedPartitionDataSize) {
    if (usedPartitionDataSize > hugePartitionSizeThreshold) {
      AbstractShuffleBuffer shuffleBuffer =
          getShuffleBufferEntry(appId, shuffleId, partitionId).getValue();
      long memoryUsed = shuffleBuffer.getSize();
      if (memoryUsed > hugePartitionMemoryLimitSize) {
        LOG.warn(
            "AppId: {}, shuffleId: {}, partitionId: {}, memory used: {}, "
                + "huge partition triggered memory limitation.",
            appId,
            shuffleId,
            partitionId,
            memoryUsed);
        return true;
      }
    }
    return false;
  }
}
