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

package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);

  private final ShuffleFlushManager shuffleFlushManager;
  private long capacity;
  private long spillThreshold;
  private long bufferSize;
  private long readCapacity;
  private int retryNum;
  private AtomicLong preAllocatedSize = new AtomicLong(0L);
  private AtomicLong inFlushSize = new AtomicLong(0L);
  private AtomicLong usedMemory = new AtomicLong(0L);
  private AtomicLong readDataMemory = new AtomicLong(0L);
  // appId -> shuffleId -> partitionId -> ShuffleBuffer to avoid too many appId
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool;


  public ShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    this.capacity = conf.getSizeAsBytes(ShuffleServerConf.SERVER_BUFFER_CAPACITY);
    this.bufferSize = conf.getSizeAsBytes(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE);
    this.spillThreshold = conf.getSizeAsBytes(ShuffleServerConf.SERVER_BUFFER_SPILL_THRESHOLD);
    this.readCapacity = conf.getSizeAsBytes(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY);
    this.shuffleFlushManager = shuffleFlushManager;
    this.bufferPool = new ConcurrentHashMap<>();
    this.retryNum = conf.getInteger(ShuffleServerConf.SERVER_MEMORY_REQUEST_RETRY_MAX);
  }

  public StatusCode registerBuffer(String appId, int shuffleId, int startPartition, int endPartition) {
    bufferPool.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    shuffleIdToBuffers.putIfAbsent(shuffleId, TreeRangeMap.create());
    RangeMap<Integer, ShuffleBuffer> bufferRangeMap = shuffleIdToBuffers.get(shuffleId);
    if (bufferRangeMap.get(startPartition) == null) {
      ShuffleServerMetrics.gaugeTotalPartitionNum.inc();
      bufferRangeMap.put(Range.closed(startPartition, endPartition), new ShuffleBuffer(bufferSize));
    } else {
      LOG.warn("Already register for appId[" + appId + "], shuffleId[" + shuffleId + "], startPartition["
          + startPartition + "], endPartition[" + endPartition + "]");
    }

    return StatusCode.SUCCESS;
  }

  public StatusCode cacheShuffleData(String appId, int shuffleId,
      boolean isPreAllocated, ShufflePartitionedData spd) {
    if (!isPreAllocated && isFull()) {
      LOG.warn("Got unexpect data, can't cache it because the space is full");
      return StatusCode.NO_BUFFER;
    }
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return StatusCode.NO_REGISTER;
    }
    RangeMap<Integer, ShuffleBuffer> rangeToBuffers = shuffleIdToBuffers.get(shuffleId);
    if (rangeToBuffers == null) {
      return StatusCode.NO_REGISTER;
    }
    Entry<Range<Integer>, ShuffleBuffer> entry = rangeToBuffers.getEntry(spd.getPartitionId());
    if (entry == null) {
      return StatusCode.NO_REGISTER;
    }
    ShuffleBuffer buffer = entry.getValue();
    Range<Integer> range = entry.getKey();
    int size = buffer.append(spd);
    updateSize(size, isPreAllocated);
    synchronized (this) {
      if (shouldFlush()) {
        LOG.info("Buffer manager is full with usedMemory[" + usedMemory.get() + "], preAllocation["
            + preAllocatedSize.get() + "], inFlush[" + inFlushSize.get() + "]");
        flush();
      } else if (buffer.isFull()) {
        LOG.info("Single buff is full with " + buffer.getSize() + " bytes");
        flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
      }
    }
    return StatusCode.SUCCESS;
  }

  public synchronized void commitShuffleTask(String appId, int shuffleId) {
    RangeMap<Integer, ShuffleBuffer> buffers = bufferPool.get(appId).get(shuffleId);
    for (Map.Entry<Range<Integer>, ShuffleBuffer> entry : buffers.asMapOfRanges().entrySet()) {
      ShuffleBuffer buffer = entry.getValue();
      Range<Integer> range = entry.getKey();
      flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
  }

  // buffer pool is full, flush all buffers
  public synchronized void flush() {
    for (Entry<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> appIdToBuffers : bufferPool.entrySet()) {
      String appId = appIdToBuffers.getKey();
      for (Entry<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers :
          appIdToBuffers.getValue().entrySet()) {
        int shuffleId = shuffleIdToBuffers.getKey();
        for (Entry<Range<Integer>, ShuffleBuffer> rangeEntry :
            shuffleIdToBuffers.getValue().asMapOfRanges().entrySet()) {
          Range<Integer> range = rangeEntry.getKey();
          flushBuffer(rangeEntry.getValue(), appId, shuffleId,
              range.lowerEndpoint(), range.upperEndpoint());
        }
      }
    }
  }

  private void flushBuffer(ShuffleBuffer buffer, String appId,
      int shuffleId, int startPartition, int endPartition) {
    ShuffleDataFlushEvent event =
        buffer.toFlushEvent(appId, shuffleId, startPartition, endPartition, () -> bufferPool.containsKey(appId));
    if (event != null) {
      inFlushSize.addAndGet(event.getSize());
      ShuffleServerMetrics.gaugeInFlushBufferSize.set(inFlushSize.get());
      shuffleFlushManager.addToFlushQueue(event);
    }
  }

  public void removeBuffer(String appId) {
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return;
    }
    // calculate released size
    long size = 0;
    for (RangeMap<Integer, ShuffleBuffer> rangeMap : shuffleIdToBuffers.values()) {
      if (rangeMap != null) {
        Collection<ShuffleBuffer> buffers = rangeMap.asMapOfRanges().values();
        if (buffers != null) {
          for (ShuffleBuffer buffer : buffers) {
            ShuffleServerMetrics.gaugeTotalPartitionNum.dec();
            size += buffer.getSize();
          }
        }
      }
    }
    // release memory
    releaseMemory(size, false, false);
    bufferPool.remove(appId);
  }

  public synchronized boolean requireMemory(long size, boolean isPreAllocated) {
    if (capacity - usedMemory.get() >= size) {
      usedMemory.addAndGet(size);
      ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());
      if (isPreAllocated) {
        requirePreAllocatedSize(size);
      }
      return true;
    }
    LOG.debug("Require memory failed with " + size + " bytes, usedMemory[" + usedMemory.get()
        + "] include preAllocation[" + preAllocatedSize.get()
        + "], inFlushSize[" + inFlushSize.get() + "]");
    return false;
  }

  public void releaseMemory(long size, boolean isReleaseFlushMemory, boolean isReleasePreAllocation) {
    if (usedMemory.get() >= size) {
      usedMemory.addAndGet(-size);
    } else {
      LOG.warn("Current allocated memory[" + usedMemory.get()
          + "] is less than released[" + size + "], set allocated memory to 0");
      usedMemory.set(0L);
    }

    ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());

    if (isReleaseFlushMemory) {
      releaseFlushMemory(size);
    }

    if (isReleasePreAllocation) {
      releasePreAllocatedSize(size);
    }
  }

  private void releaseFlushMemory(long size) {
    if (inFlushSize.get() >= size) {
      inFlushSize.addAndGet(-size);
    } else {
      LOG.warn("Current in flush memory[" + inFlushSize.get()
          + "] is less than released[" + size + "], set allocated memory to 0");
      inFlushSize.set(0L);
    }
    ShuffleServerMetrics.gaugeInFlushBufferSize.set(inFlushSize.get());
  }

  public boolean requireReadMemoryWithRetry(long size) {
    for (int i = 0; i < retryNum; i++) {
      synchronized (this) {
        if (readDataMemory.get() + size < readCapacity) {
          readDataMemory.addAndGet(size);
          return true;
        }
      }
      LOG.info("Can't require[" + size + "] for read data, current[" + readDataMemory.get()
          + "], capacity[" + readCapacity + "], re-try " + i + " times");
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        LOG.warn("Error happened when require memory", e);
      }
    }
    return false;
  }

  public void releaseReadMemory(long size) {
    if (readDataMemory.get() >= size) {
      readDataMemory.addAndGet(-size);
    } else {
      LOG.warn("Current read memory[" + readDataMemory.get()
          + "] is less than released[" + size + "], set read memory to 0");
      readDataMemory.set(0L);
    }
  }

  void updateSize(long delta, boolean isPreAllocated) {
    if (isPreAllocated) {
      releasePreAllocatedSize(delta);
    } else {
      // add size if not allocated
      usedMemory.addAndGet(delta);
      ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());
    }
  }

  void requirePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(delta);
    ShuffleServerMetrics.gaugeAllocatedBufferSize.set(preAllocatedSize.get());
  }

  void releasePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(-delta);
    ShuffleServerMetrics.gaugeAllocatedBufferSize.set(preAllocatedSize.get());
  }

  // if data size in buffer > spillThreshold, do the flush
  boolean shouldFlush() {
    return usedMemory.get() - preAllocatedSize.get() - inFlushSize.get() > spillThreshold;
  }

  boolean isFull() {
    return usedMemory.get() >= capacity;
  }

  @VisibleForTesting
  public Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> getBufferPool() {
    return bufferPool;
  }

  public long getUsedMemory() {
    return usedMemory.get();
  }

  public long getInFlushSize() {
    return inFlushSize.get();
  }

  public long getCapacity() {
    return capacity;
  }

  @VisibleForTesting
  void resetSize() {
    usedMemory = new AtomicLong(0L);
    preAllocatedSize = new AtomicLong(0L);
    inFlushSize = new AtomicLong(0L);
  }

  public long getPreAllocatedSize() {
    return preAllocatedSize.get();
  }

}
