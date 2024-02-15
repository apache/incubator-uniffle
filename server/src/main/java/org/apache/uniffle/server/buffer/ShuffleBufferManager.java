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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;
import io.netty.util.internal.PlatformDependent;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleFlushManager;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_TEST_MODE_ENABLE;

public class ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);

  private ShuffleTaskManager shuffleTaskManager;
  private final ShuffleFlushManager shuffleFlushManager;
  private long capacity;
  private long readCapacity;
  private int retryNum;
  private long highWaterMark;
  private long highWaterMarkForDirectMemory;
  private long lowWaterMark;
  private long lowWaterMarkForDirectMemory;
  private boolean bufferFlushEnabled;
  private long bufferFlushThreshold;
  // when shuffle buffer manager flushes data, shuffles with data size < shuffleFlushThreshold is
  // kept in memory to
  // reduce small I/Os to persistent storage, especially for local HDDs.
  private long shuffleFlushThreshold;
  // Huge partition vars
  private long hugePartitionSizeThreshold;
  private long hugePartitionMemoryLimitSize;

  // vars for server_type: GRPC_NETTY
  private boolean nettyServerEnabled;
  private long reservedOnHeapPreAllocatedBufferBytes;
  private long reservedOffHeapPreAllocatedBufferBytes;
  private long maxAvailableOffHeapBytes;
  private boolean testMode;

  protected long bufferSize = 0;
  protected AtomicLong preAllocatedSize = new AtomicLong(0L);
  protected AtomicLong inFlushSize = new AtomicLong(0L);
  protected AtomicLong usedMemory = new AtomicLong(0L);
  private AtomicLong readDataMemory = new AtomicLong(0L);
  // appId -> shuffleId -> partitionId -> ShuffleBuffer to avoid too many appId
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool;
  // appId -> shuffleId -> shuffle size in buffer
  protected Map<String, Map<Integer, AtomicLong>> shuffleSizeMap = JavaUtils.newConcurrentMap();

  public ShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    this.nettyServerEnabled = conf.get(ShuffleServerConf.NETTY_SERVER_PORT) >= 0;
    long heapSize = Runtime.getRuntime().maxMemory();
    this.capacity = conf.getSizeAsBytes(ShuffleServerConf.SERVER_BUFFER_CAPACITY);
    if (this.capacity < 0) {
      this.capacity =
          nettyServerEnabled
              ? (long)
                  (NettyUtils.getMaxDirectMemory()
                      * conf.getDouble(ShuffleServerConf.SERVER_BUFFER_CAPACITY_RATIO))
              : (long) (heapSize * conf.getDouble(ShuffleServerConf.SERVER_BUFFER_CAPACITY_RATIO));
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
    this.shuffleFlushManager = shuffleFlushManager;
    this.bufferPool = new ConcurrentHashMap<>();
    this.retryNum = conf.getInteger(ShuffleServerConf.SERVER_MEMORY_REQUEST_RETRY_MAX);
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
    this.reservedOnHeapPreAllocatedBufferBytes =
        conf.getSizeAsBytes(ShuffleServerConf.SERVER_PRE_ALLOCATION_RESERVED_ON_HEAP_SIZE);
    this.reservedOffHeapPreAllocatedBufferBytes =
        conf.getSizeAsBytes(ShuffleServerConf.SERVER_PRE_ALLOCATION_RESERVED_OFF_HEAP_SIZE);
    this.testMode = conf.getBoolean(RSS_TEST_MODE_ENABLE);
    if (nettyServerEnabled) {
      if (heapSize < this.reservedOnHeapPreAllocatedBufferBytes) {
        throw new IllegalArgumentException(
            "The reserved on-heap memory size for pre-allocated buffer "
                + "should not be greater than the max value of heap memory");
      }
      if (capacity < this.reservedOffHeapPreAllocatedBufferBytes) {
        throw new IllegalArgumentException(
            "The reserved off-heap memory size for pre-allocated buffer "
                + "should not be greater than the capacity of direct memory");
      }
    }
    this.maxAvailableOffHeapBytes = capacity - this.reservedOffHeapPreAllocatedBufferBytes;
    this.highWaterMarkForDirectMemory =
        (long)
            (this.maxAvailableOffHeapBytes
                / 100.0
                * conf.get(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE));
    this.lowWaterMarkForDirectMemory =
        (long)
            (this.maxAvailableOffHeapBytes
                / 100.0
                * conf.get(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE));
    this.bufferFlushEnabled = conf.getBoolean(ShuffleServerConf.SINGLE_BUFFER_FLUSH_ENABLED);
    this.bufferFlushThreshold =
        conf.getSizeAsBytes(ShuffleServerConf.SINGLE_BUFFER_FLUSH_THRESHOLD);
    this.shuffleFlushThreshold =
        conf.getSizeAsBytes(ShuffleServerConf.SERVER_SHUFFLE_FLUSH_THRESHOLD);
    this.hugePartitionSizeThreshold =
        conf.getSizeAsBytes(ShuffleServerConf.HUGE_PARTITION_SIZE_THRESHOLD);
    this.hugePartitionMemoryLimitSize =
        Math.round(
            capacity * conf.get(ShuffleServerConf.HUGE_PARTITION_MEMORY_USAGE_LIMITATION_RATIO));
  }

  public void setShuffleTaskManager(ShuffleTaskManager taskManager) {
    this.shuffleTaskManager = taskManager;
  }

  public StatusCode registerBuffer(
      String appId, int shuffleId, int startPartition, int endPartition) {
    bufferPool.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    shuffleIdToBuffers.computeIfAbsent(shuffleId, key -> TreeRangeMap.create());
    RangeMap<Integer, ShuffleBuffer> bufferRangeMap = shuffleIdToBuffers.get(shuffleId);
    if (bufferRangeMap.get(startPartition) == null) {
      ShuffleServerMetrics.counterTotalPartitionNum.inc();
      ShuffleServerMetrics.gaugeTotalPartitionNum.inc();
      bufferRangeMap.put(Range.closed(startPartition, endPartition), new ShuffleBuffer(bufferSize));
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

  public StatusCode cacheShuffleData(
      String appId, int shuffleId, boolean isPreAllocated, ShufflePartitionedData spd) {
    return cacheShuffleData(appId, shuffleId, isPreAllocated, spd, 0);
  }

  public StatusCode cacheShuffleData(
      String appId,
      int shuffleId,
      boolean isPreAllocated,
      ShufflePartitionedData spd,
      int avgEstimatedSize) {
    if (!isPreAllocated && isFull()) {
      LOG.warn("Got unexpected data, can't cache it because the space is full");
      return StatusCode.NO_BUFFER;
    }

    Entry<Range<Integer>, ShuffleBuffer> entry =
        getShuffleBufferEntry(appId, shuffleId, spd.getPartitionId());
    if (entry == null) {
      return StatusCode.NO_REGISTER;
    }

    ShuffleBuffer buffer = entry.getValue();
    long size = buffer.append(spd, avgEstimatedSize);
    long bufferSize = nettyServerEnabled ? avgEstimatedSize : size;
    if (!isPreAllocated) {
      updateUsedMemory(bufferSize);
    }
    updateShuffleSize(appId, shuffleId, bufferSize);
    synchronized (this) {
      flushSingleBufferIfNecessary(
          buffer,
          appId,
          shuffleId,
          spd.getPartitionId(),
          entry.getKey().lowerEndpoint(),
          entry.getKey().upperEndpoint());
      flushIfNecessary();
    }
    return StatusCode.SUCCESS;
  }

  private void updateShuffleSize(String appId, int shuffleId, long size) {
    shuffleSizeMap.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, AtomicLong> shuffleIdToSize = shuffleSizeMap.get(appId);
    shuffleIdToSize.computeIfAbsent(shuffleId, key -> new AtomicLong(0));
    shuffleIdToSize.get(shuffleId).addAndGet(size);
  }

  public Entry<Range<Integer>, ShuffleBuffer> getShuffleBufferEntry(
      String appId, int shuffleId, int partitionId) {
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return null;
    }
    RangeMap<Integer, ShuffleBuffer> rangeToBuffers = shuffleIdToBuffers.get(shuffleId);
    if (rangeToBuffers == null) {
      return null;
    }
    Entry<Range<Integer>, ShuffleBuffer> entry = rangeToBuffers.getEntry(partitionId);
    if (entry == null) {
      return null;
    }
    return entry;
  }

  public ShuffleDataResult getShuffleData(
      String appId, int shuffleId, int partitionId, long blockId, int readBufferSize) {
    return getShuffleData(appId, shuffleId, partitionId, blockId, readBufferSize, null);
  }

  public ShuffleDataResult getShuffleData(
      String appId,
      int shuffleId,
      int partitionId,
      long blockId,
      int readBufferSize,
      Roaring64NavigableMap expectedTaskIds) {
    Map.Entry<Range<Integer>, ShuffleBuffer> entry =
        getShuffleBufferEntry(appId, shuffleId, partitionId);
    if (entry == null) {
      return null;
    }

    ShuffleBuffer buffer = entry.getValue();
    if (buffer == null) {
      return null;
    }
    return buffer.getShuffleData(blockId, readBufferSize, expectedTaskIds);
  }

  void flushSingleBufferIfNecessary(
      ShuffleBuffer buffer,
      String appId,
      int shuffleId,
      int partitionId,
      int startPartition,
      int endPartition) {
    boolean isHugePartition = isHugePartition(appId, shuffleId, partitionId);
    // When we use multi storage and trigger single buffer flush, the buffer size should be bigger
    // than rss.server.flush.cold.storage.threshold.size, otherwise cold storage will be useless.
    long size = nettyServerEnabled ? buffer.getEstimatedSize() : buffer.getSize();
    if ((isHugePartition || this.bufferFlushEnabled) && size > this.bufferFlushThreshold) {
      flushBuffer(buffer, appId, shuffleId, startPartition, endPartition, isHugePartition);
      return;
    }
  }

  public void flushIfNecessary() {
    // if data size in buffer > highWaterMark, do the flush
    long pinnedDirectMemory = getPinnedDirectMemory();
    long usedDirectMemory = PlatformDependent.usedDirectMemory();
    long usedHeapMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    boolean needFlush;
    if (nettyServerEnabled) {
      needFlush = pinnedDirectMemory > highWaterMarkForDirectMemory;
    } else {
      needFlush = usedMemory.get() - preAllocatedSize.get() - inFlushSize.get() > highWaterMark;
    }
    if (needFlush) {
      // todo: add a metric here to track how many times flush occurs.
      LOG.info(
          "Start to flush with usedMemory[{}], preAllocatedSize[{}], inFlushSize[{}], usedDirectMemory[{}], usedHeapMemory[{}], pinnedDirectMemory[{}]",
          usedMemory.get(),
          preAllocatedSize.get(),
          inFlushSize.get(),
          usedDirectMemory,
          usedHeapMemory,
          pinnedDirectMemory);
      Map<String, Set<Integer>> pickedShuffle = pickFlushedShuffle();
      flush(pickedShuffle);
    }
  }

  public synchronized void commitShuffleTask(String appId, int shuffleId) {
    RangeMap<Integer, ShuffleBuffer> buffers = bufferPool.get(appId).get(shuffleId);
    for (Map.Entry<Range<Integer>, ShuffleBuffer> entry : buffers.asMapOfRanges().entrySet()) {
      ShuffleBuffer buffer = entry.getValue();
      Range<Integer> range = entry.getKey();
      flushBuffer(
          buffer,
          appId,
          shuffleId,
          range.lowerEndpoint(),
          range.upperEndpoint(),
          isHugePartition(appId, shuffleId, range.lowerEndpoint()));
    }
  }

  protected void flushBuffer(
      ShuffleBuffer buffer,
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
      long eventSize = nettyServerEnabled ? event.getEstimatedSize() : event.getSize();
      event.addCleanupCallback(() -> releaseMemory(eventSize, true, false));
      updateShuffleSize(appId, shuffleId, -eventSize);
      inFlushSize.addAndGet(eventSize);
      if (isHugePartition) {
        event.markOwnedByHugePartition();
      }
      ShuffleServerMetrics.gaugeInFlushBufferSize.set(inFlushSize.get());
      shuffleFlushManager.addToFlushQueue(event);
    }
  }

  public void removeBuffer(String appId) {
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return;
    }
    removeBufferByShuffleId(appId, shuffleIdToBuffers.keySet());
    shuffleSizeMap.remove(appId);
    bufferPool.remove(appId);
  }

  public synchronized boolean requireMemory(long size, boolean isPreAllocated) {
    long pinnedDirectMemory = getPinnedDirectMemory();
    long usedDirectMemory = PlatformDependent.usedDirectMemory();
    long usedHeapMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    boolean canAllocate;
    if (nettyServerEnabled) {
      boolean canAllocateDirectBuffer = this.maxAvailableOffHeapBytes - pinnedDirectMemory >= size;
      boolean isSufficientOnHeapMemory =
          Runtime.getRuntime().maxMemory() - usedHeapMemory
              >= reservedOnHeapPreAllocatedBufferBytes;
      canAllocate = canAllocateDirectBuffer && isSufficientOnHeapMemory;
    } else {
      canAllocate = capacity - usedMemory.get() >= size;
    }
    if (canAllocate) {
      if (nettyServerEnabled) {
        usedMemory.lazySet(pinnedDirectMemory);
      } else {
        usedMemory.addAndGet(size);
      }
      ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());
      if (isPreAllocated) {
        requirePreAllocatedSize(size);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Require memory succeeded with "
                + size
                + " bytes, usedMemory["
                + usedMemory.get()
                + "] include preAllocation["
                + preAllocatedSize.get()
                + "], inFlushSize["
                + inFlushSize.get()
                + "], usedDirectMemory["
                + usedDirectMemory
                + "], usedHeapMemory["
                + usedHeapMemory
                + "]");
      }
      return true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Require memory failed with "
              + size
              + " bytes, usedMemory["
              + usedMemory.get()
              + "] include preAllocation["
              + preAllocatedSize.get()
              + "], inFlushSize["
              + inFlushSize.get()
              + "], usedDirectMemory["
              + usedDirectMemory
              + "], usedHeapMemory["
              + usedHeapMemory
              + "]");
    }
    return false;
  }

  public void releaseMemory(
      long size, boolean isReleaseFlushMemory, boolean isReleasePreAllocation) {
    if (nettyServerEnabled) {
      usedMemory.lazySet(getPinnedDirectMemory());
    } else {
      if (usedMemory.get() >= size) {
        usedMemory.addAndGet(-size);
      } else {
        LOG.warn(
            "Current allocated memory["
                + usedMemory.get()
                + "] is less than released["
                + size
                + "], set allocated memory to 0");
        usedMemory.set(0L);
      }
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
      LOG.warn(
          "Current in flush memory["
              + inFlushSize.get()
              + "] is less than released["
              + size
              + "], set allocated memory to 0");
      inFlushSize.set(0L);
    }
    ShuffleServerMetrics.gaugeInFlushBufferSize.set(inFlushSize.get());
  }

  public boolean requireReadMemoryWithRetry(long size) {
    ShuffleServerMetrics.counterTotalRequireReadMemoryNum.inc();
    for (int i = 0; i < retryNum; i++) {
      synchronized (this) {
        if (readDataMemory.get() + size < readCapacity) {
          readDataMemory.addAndGet(size);
          ShuffleServerMetrics.gaugeReadBufferUsedSize.inc(size);
          return true;
        }
      }
      LOG.info(
          "Can't require["
              + size
              + "] for read data, current["
              + readDataMemory.get()
              + "], capacity["
              + readCapacity
              + "], re-try "
              + i
              + " times");
      ShuffleServerMetrics.counterTotalRequireReadMemoryRetryNum.inc();
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        LOG.warn("Error happened when require memory", e);
      }
    }
    ShuffleServerMetrics.counterTotalRequireReadMemoryFailedNum.inc();
    return false;
  }

  public void releaseReadMemory(long size) {
    if (readDataMemory.get() >= size) {
      readDataMemory.addAndGet(-size);
      ShuffleServerMetrics.gaugeReadBufferUsedSize.dec(size);
    } else {
      LOG.warn(
          "Current read memory["
              + readDataMemory.get()
              + "] is less than released["
              + size
              + "], set read memory to 0");
      readDataMemory.set(0L);
      ShuffleServerMetrics.gaugeReadBufferUsedSize.set(0);
    }
  }

  // flush the buffer with required map which is <appId -> shuffleId>
  public synchronized void flush(Map<String, Set<Integer>> requiredFlush) {
    for (Map.Entry<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> appIdToBuffers :
        bufferPool.entrySet()) {
      String appId = appIdToBuffers.getKey();
      if (requiredFlush.containsKey(appId)) {
        for (Map.Entry<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers :
            appIdToBuffers.getValue().entrySet()) {
          int shuffleId = shuffleIdToBuffers.getKey();
          Set<Integer> requiredShuffleId = requiredFlush.get(appId);
          if (requiredShuffleId != null && requiredShuffleId.contains(shuffleId)) {
            for (Map.Entry<Range<Integer>, ShuffleBuffer> rangeEntry :
                shuffleIdToBuffers.getValue().asMapOfRanges().entrySet()) {
              Range<Integer> range = rangeEntry.getKey();
              flushBuffer(
                  rangeEntry.getValue(),
                  appId,
                  shuffleId,
                  range.lowerEndpoint(),
                  range.upperEndpoint(),
                  isHugePartition(appId, shuffleId, range.lowerEndpoint()));
            }
          }
        }
      }
    }
  }

  public void updateUsedMemory(long delta) {
    if (nettyServerEnabled) {
      usedMemory.lazySet(getPinnedDirectMemory());
    } else {
      // add size if not allocated
      usedMemory.addAndGet(delta);
    }
    ShuffleServerMetrics.gaugeUsedBufferSize.set(usedMemory.get());
  }

  void requirePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(delta);
    ShuffleServerMetrics.gaugeAllocatedBufferSize.set(preAllocatedSize.get());
  }

  public void releasePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(-delta);
    ShuffleServerMetrics.gaugeAllocatedBufferSize.set(preAllocatedSize.get());
  }

  boolean isFull() {
    return nettyServerEnabled ? getPinnedDirectMemory() >= capacity : usedMemory.get() >= capacity;
  }

  @VisibleForTesting
  public Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> getBufferPool() {
    return bufferPool;
  }

  @VisibleForTesting
  public ShuffleBuffer getShuffleBuffer(String appId, int shuffleId, int partitionId) {
    return getShuffleBufferEntry(appId, shuffleId, partitionId).getValue();
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
  public long getReadCapacity() {
    return readCapacity;
  }

  @VisibleForTesting
  public void resetSize() {
    usedMemory = new AtomicLong(0L);
    preAllocatedSize = new AtomicLong(0L);
    inFlushSize = new AtomicLong(0L);
  }

  @VisibleForTesting
  public Map<String, Map<Integer, AtomicLong>> getShuffleSizeMap() {
    return shuffleSizeMap;
  }

  public long getPreAllocatedSize() {
    return preAllocatedSize.get();
  }

  // sort for shuffle according to data size, then pick properly data which will be flushed
  private Map<String, Set<Integer>> pickFlushedShuffle() {
    // create list for sort
    List<Entry<String, AtomicLong>> sizeList = generateSizeList();
    sizeList.sort(
        (entry1, entry2) -> {
          if (entry1 == null && entry2 == null) {
            return 0;
          }
          if (entry1 == null) {
            return 1;
          }
          if (entry2 == null) {
            return -1;
          }
          if (entry1.getValue().get() > entry2.getValue().get()) {
            return -1;
          } else if (entry1.getValue().get() == entry2.getValue().get()) {
            return 0;
          }
          return 1;
        });

    Map<String, Set<Integer>> pickedShuffle = Maps.newHashMap();
    // The algorithm here is to flush data size > highWaterMark - lowWaterMark
    // the remaining data in buffer maybe more than lowWaterMark
    // because shuffle server is still receiving data, but it should be ok
    long expectedFlushSize =
        nettyServerEnabled
            ? highWaterMarkForDirectMemory - lowWaterMarkForDirectMemory
            : highWaterMark - lowWaterMark;
    long atLeastFlushSizeIgnoreThreshold = expectedFlushSize >>> 1;
    long pickedFlushSize = 0L;
    int printIndex = 0;
    int printIgnoreIndex = 0;
    int printMax = 10;
    for (Map.Entry<String, AtomicLong> entry : sizeList) {
      long size = entry.getValue().get();
      String appIdShuffleIdKey = entry.getKey();
      if (size > this.shuffleFlushThreshold || pickedFlushSize <= atLeastFlushSizeIgnoreThreshold) {
        pickedFlushSize += size;
        addPickedShuffle(appIdShuffleIdKey, pickedShuffle);
        // print detail picked info
        if (printIndex < printMax) {
          LOG.info("Pick application_shuffleId[{}] with {} bytes", appIdShuffleIdKey, size);
          printIndex++;
        }
        if (pickedFlushSize > expectedFlushSize) {
          LOG.info("Finish flush pick with {} bytes", pickedFlushSize);
          break;
        }
      } else {
        // since shuffle size is ordered by size desc, we can skip process more shuffle data once
        // some shuffle's size
        // is less than threshold
        if (printIgnoreIndex < printMax) {
          LOG.info("Ignore application_shuffleId[{}] with {} bytes", appIdShuffleIdKey, size);
          printIgnoreIndex++;
        } else {
          break;
        }
      }
    }
    return pickedShuffle;
  }

  private List<Map.Entry<String, AtomicLong>> generateSizeList() {
    Map<String, AtomicLong> sizeMap = Maps.newHashMap();
    for (Map.Entry<String, Map<Integer, AtomicLong>> appEntry : shuffleSizeMap.entrySet()) {
      String appId = appEntry.getKey();
      for (Map.Entry<Integer, AtomicLong> shuffleEntry : appEntry.getValue().entrySet()) {
        Integer shuffleId = shuffleEntry.getKey();
        sizeMap.put(RssUtils.generateShuffleKey(appId, shuffleId), shuffleEntry.getValue());
      }
    }
    return Lists.newArrayList(sizeMap.entrySet());
  }

  private void addPickedShuffle(String shuffleIdKey, Map<String, Set<Integer>> pickedShuffle) {
    String[] splits = shuffleIdKey.split(Constants.KEY_SPLIT_CHAR);
    String appId = splits[0];
    Integer shuffleId = Integer.parseInt(splits[1]);
    pickedShuffle.computeIfAbsent(appId, key -> Sets.newHashSet());
    Set<Integer> shuffleIdSet = pickedShuffle.get(appId);
    shuffleIdSet.add(shuffleId);
  }

  private long getPinnedDirectMemory() {
    if (testMode) {
      // In test mode, there won't be significant concurrency pressure,
      // so it's sufficient to simply return the actual pinned direct memory immediately
      long pinnedDirectMemory = NettyUtils.getNettyBufferAllocator().pinnedDirectMemory();
      return pinnedDirectMemory == -1L ? 0L : pinnedDirectMemory;
    }
    // To improve performance, return a periodically retrieved pinned direct memory
    long pinnedDirectMemory =
        (long) ShuffleServerMetrics.gaugePinnedDirectMemorySize.get() == -1L
            ? 0L
            : (long) ShuffleServerMetrics.gaugePinnedDirectMemorySize.get();
    return pinnedDirectMemory;
  }

  public void removeBufferByShuffleId(String appId, Collection<Integer> shuffleIds) {
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return;
    }

    Map<Integer, AtomicLong> shuffleIdToSizeMap = shuffleSizeMap.get(appId);
    for (int shuffleId : shuffleIds) {
      long size = 0;

      RangeMap<Integer, ShuffleBuffer> bufferRangeMap = shuffleIdToBuffers.remove(shuffleId);
      if (bufferRangeMap == null) {
        continue;
      }
      Collection<ShuffleBuffer> buffers = bufferRangeMap.asMapOfRanges().values();
      if (buffers != null) {
        for (ShuffleBuffer buffer : buffers) {
          buffer.getBlocks().forEach(spb -> spb.getData().release());
          ShuffleServerMetrics.gaugeTotalPartitionNum.dec();
          size += nettyServerEnabled ? buffer.getEstimatedSize() : buffer.getSize();
        }
      }
      releaseMemory(size, false, false);
      if (shuffleIdToSizeMap != null) {
        shuffleIdToSizeMap.remove(shuffleId);
      }
    }
  }

  boolean isHugePartition(String appId, int shuffleId, int partitionId) {
    return shuffleTaskManager != null
        && shuffleTaskManager.getPartitionDataSize(appId, shuffleId, partitionId)
            > hugePartitionSizeThreshold;
  }

  public boolean isHugePartition(long usedPartitionDataSize) {
    return usedPartitionDataSize > hugePartitionSizeThreshold;
  }

  public boolean limitHugePartition(
      String appId, int shuffleId, int partitionId, long usedPartitionDataSize) {
    if (usedPartitionDataSize > hugePartitionSizeThreshold) {
      ShuffleBuffer shuffleBuffer = getShuffleBufferEntry(appId, shuffleId, partitionId).getValue();
      long memoryUsed =
          nettyServerEnabled ? shuffleBuffer.getEstimatedSize() : shuffleBuffer.getSize();
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
