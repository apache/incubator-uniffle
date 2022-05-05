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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.server.buffer.PreAllocatedBufferInfo;
import com.tencent.rss.server.buffer.ShuffleBufferManager;
import com.tencent.rss.server.storage.StorageManager;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.common.StorageReadMetrics;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final ShuffleFlushManager shuffleFlushManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ScheduledExecutorService expiredAppCleanupExecutorService;
  private final StorageManager storageManager;
  private AtomicLong requireBufferId = new AtomicLong(0);
  private ShuffleServerConf conf;
  private long appExpiredWithoutHB;
  private long preAllocationExpired;
  private long commitCheckIntervalMax;
  // appId -> shuffleId -> blockIds to avoid too many appId
  // store taskAttemptId info to filter speculation task
  // Roaring64NavigableMap instance will cost much memory,
  // merge different blockId of partition to one bitmap can reduce memory cost,
  // but when get blockId, performance will degrade a little which can be optimized by client configuration
  private Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds;
  private ShuffleBufferManager shuffleBufferManager;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  // appId -> shuffleId -> commit count
  private Map<String, Map<Long, AtomicInteger>> commitCounts = Maps.newConcurrentMap();
  private Map<String, Map<Integer, Object>> commitLocks = Maps.newConcurrentMap();
  // appId -> shuffleId -> blockIds
  private Map<String, Map<Integer, Roaring64NavigableMap>> cachedBlockIds = Maps.newConcurrentMap();
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds = Maps.newConcurrentMap();
  private Runnable clearResourceThread;
  private BlockingQueue<String> expiredAppIdQueue = Queues.newLinkedBlockingQueue();
  // appId -> shuffleId -> serverReadHandler

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager,
      StorageManager storageManager) {
    this.conf = conf;
    this.shuffleFlushManager = shuffleFlushManager;
    this.partitionsToBlockIds = Maps.newConcurrentMap();
    this.shuffleBufferManager = shuffleBufferManager;
    this.storageManager = storageManager;
    this.appExpiredWithoutHB = conf.getLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT);
    this.commitCheckIntervalMax = conf.getLong(ShuffleServerConf.SERVER_COMMIT_CHECK_INTERVAL_MAX);
    this.preAllocationExpired = conf.getLong(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED);
    // the thread for checking application status
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("checkResource-%d").build());
    scheduledExecutorService.scheduleAtFixedRate(
        () -> preAllocatedBufferCheck(), preAllocationExpired / 2,
        preAllocationExpired / 2, TimeUnit.MILLISECONDS);
    this.expiredAppCleanupExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("expiredAppCleaner").build());
    expiredAppCleanupExecutorService.scheduleAtFixedRate(
        () -> checkResourceStatus(), appExpiredWithoutHB / 2,
        appExpiredWithoutHB / 2, TimeUnit.MILLISECONDS);
    // the thread for clear expired resources
    clearResourceThread = () -> {
      while (true) {
        try {
          String appId = expiredAppIdQueue.take();
          removeResources(appId);
        } catch (Exception e) {
          LOG.error("Exception happened when clear resource for expired application", e);
        }
      }
    };
    Thread thread = new Thread(clearResourceThread);
    thread.setName("clearResourceThread");
    thread.start();
  }

  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      String remoteStorage) {
    refreshAppId(appId);
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    for (PartitionRange partitionRange : partitionRanges) {
      shuffleBufferManager.registerBuffer(appId, shuffleId, partitionRange.getStart(), partitionRange.getEnd());
    }
    if (!StringUtils.isEmpty(remoteStorage)) {
      storageManager.registerRemoteStorage(appId, remoteStorage);
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode cacheShuffleData(
      String appId, int shuffleId, boolean isPreAllocated, ShufflePartitionedData spd) {
    refreshAppId(appId);
    return shuffleBufferManager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
  }

  public boolean isPreAllocated(long requireBufferId) {
    return requireBufferIds.containsKey(requireBufferId);
  }

  public void removeRequireBufferId(long requireId) {
    requireBufferIds.remove(requireId);
  }

  public StatusCode commitShuffle(String appId, int shuffleId) throws Exception {
    long start = System.currentTimeMillis();
    refreshAppId(appId);
    Roaring64NavigableMap cachedBlockIds = getCachedBlockIds(appId, shuffleId);
    Roaring64NavigableMap cloneBlockIds;
    commitLocks.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, Object> shuffleLevelLocks = commitLocks.get(appId);
    shuffleLevelLocks.putIfAbsent(shuffleId, new Object());
    Object lock = shuffleLevelLocks.get(shuffleId);
    synchronized (lock) {
      long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      byte[] bitmapBytes;
      synchronized (cachedBlockIds) {
        bitmapBytes = RssUtils.serializeBitMap(cachedBlockIds);
      }
      cloneBlockIds = RssUtils.deserializeBitMap(bitmapBytes);
      long expectedCommitted = cloneBlockIds.getLongCardinality();
      shuffleBufferManager.commitShuffleTask(appId, shuffleId);
      Roaring64NavigableMap committedBlockIds;
      Roaring64NavigableMap cloneCommittedBlockIds;
      long checkInterval = 1000L;
      while (true) {
        committedBlockIds = shuffleFlushManager.getCommittedBlockIds(appId, shuffleId);
        synchronized (committedBlockIds) {
          bitmapBytes = RssUtils.serializeBitMap(committedBlockIds);
        }
        cloneCommittedBlockIds = RssUtils.deserializeBitMap(bitmapBytes);
        cloneBlockIds.andNot(cloneCommittedBlockIds);
        if (cloneBlockIds.isEmpty()) {
          break;
        }
        Thread.sleep(checkInterval);
        if (System.currentTimeMillis() - start > commitTimeout) {
          throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
        }
        LOG.info("Checking commit result for appId[" + appId + "], shuffleId[" + shuffleId
            + "], expect committed[" + expectedCommitted
            + "], remain[" + cloneBlockIds.getLongCardinality() + "]");
        checkInterval = Math.min(checkInterval * 2, commitCheckIntervalMax);
      }
      LOG.info("Finish commit for appId[" + appId + "], shuffleId[" + shuffleId
          + "] with expectedCommitted[" + expectedCommitted + "], cost "
          + (System.currentTimeMillis() - start) + " ms to check");
    }
    return StatusCode.SUCCESS;
  }

  public void addFinishedBlockIds(
      String appId, Integer shuffleId, Map<Integer, long[]> partitionToBlockIds, int bitmapNum) {
    refreshAppId(appId);
    Map<Integer, Roaring64NavigableMap[]> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (!shuffleIdToPartitions.containsKey(shuffleId)) {
      Roaring64NavigableMap[] blockIds = new Roaring64NavigableMap[bitmapNum];
      for (int i = 0; i < bitmapNum; i++) {
        blockIds[i] = Roaring64NavigableMap.bitmapOf();
      }
      shuffleIdToPartitions.putIfAbsent(shuffleId, blockIds);
    }
    Roaring64NavigableMap[] blockIds = shuffleIdToPartitions.get(shuffleId);
    for (Map.Entry<Integer, long[]> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      Roaring64NavigableMap bitmap = blockIds[partitionId % bitmapNum];
      synchronized (bitmap) {
        for (long blockId : entry.getValue()) {
          bitmap.addLong(blockId);
        }
      }
    }
  }

  public int updateAndGetCommitCount(String appId, long shuffleId) {
    commitCounts.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Long, AtomicInteger> shuffleCommit = commitCounts.get(appId);
    shuffleCommit.putIfAbsent(shuffleId, new AtomicInteger(0));
    AtomicInteger commitNum = shuffleCommit.get(shuffleId);
    return commitNum.incrementAndGet();
  }

  public void updateCachedBlockIds(String appId, int shuffleId, ShufflePartitionedBlock[] spbs) {
    if (spbs == null || spbs.length == 0) {
      return;
    }
    cachedBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, Roaring64NavigableMap> shuffleToBlockIds = cachedBlockIds.get(appId);
    shuffleToBlockIds.putIfAbsent(shuffleId, Roaring64NavigableMap.bitmapOf());
    Roaring64NavigableMap bitmap = shuffleToBlockIds.get(shuffleId);
    synchronized (bitmap) {
      for (ShufflePartitionedBlock spb : spbs) {
        bitmap.addLong(spb.getBlockId());
      }
    }
  }

  public Roaring64NavigableMap getCachedBlockIds(String appId, int shuffleId) {
    Map<Integer, Roaring64NavigableMap> shuffleIdToBlockIds = cachedBlockIds.get(appId);
    if (shuffleIdToBlockIds == null) {
      LOG.warn("Unexpected value when getCachedBlockIds for appId[" + appId + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    Roaring64NavigableMap blockIds = shuffleIdToBlockIds.get(shuffleId);
    if (blockIds == null) {
      LOG.warn("Unexpected value when getCachedBlockIds for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    return blockIds;
  }

  public long requireBuffer(int requireSize) {
    long requireId = -1;
    if (shuffleBufferManager.requireMemory(requireSize, true)) {
      requireId = requireBufferId.incrementAndGet();
      requireBufferIds.put(requireId,
          new PreAllocatedBufferInfo(requireId, System.currentTimeMillis(), requireSize));
    }
    return requireId;
  }

  public byte[] getFinishedBlockIds(
      String appId, Integer shuffleId, Integer partitionId) throws IOException {
    refreshAppId(appId);
    Storage storage = storageManager.selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId));
    // update shuffle's timestamp that was recently read.
    storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));

    Map<Integer, Roaring64NavigableMap[]> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      return null;
    }
    Roaring64NavigableMap[] blockIds = shuffleIdToPartitions.get(shuffleId);
    if (blockIds == null) {
      return new byte[]{};
    }
    Roaring64NavigableMap bitmap = blockIds[partitionId % blockIds.length];
    if (bitmap == null) {
      return new byte[]{};
    }

    if (partitionId > Constants.MAX_PARTITION_ID) {
      throw new RuntimeException("Get invalid partitionId[" + partitionId
          + "] which greater than " + Constants.MAX_PARTITION_ID);
    }

    return RssUtils.serializeBitMap(getBlockIdsByPartitionId(partitionId, bitmap));
  }

  // partitionId is passed as long to calculate minValue/maxValue
  protected Roaring64NavigableMap getBlockIdsByPartitionId(long partitionId, Roaring64NavigableMap bitmap) {
    Roaring64NavigableMap result = Roaring64NavigableMap.bitmapOf();
    LongIterator iter = bitmap.getLongIterator();
    long minValue = partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH;
    long maxValue = Long.MAX_VALUE;
    if (partitionId < Constants.MAX_PARTITION_ID) {
      maxValue = (partitionId + 1) << (Constants.TASK_ATTEMPT_ID_MAX_LENGTH);
    }
    long mask = (1L << (Constants.TASK_ATTEMPT_ID_MAX_LENGTH + Constants.PARTITION_ID_MAX_LENGTH)) - 1;
    while (iter.hasNext()) {
      long blockId = iter.next();
      long partitionAndTask = blockId & mask;
      if (partitionAndTask >= minValue && partitionAndTask < maxValue) {
        result.addLong(blockId);
      }
    }
    return result;
  }

  public ShuffleDataResult getInMemoryShuffleData(
      String appId, Integer shuffleId, Integer partitionId, long blockId, int readBufferSize) {
    return shuffleBufferManager.getShuffleData(appId,
        shuffleId, partitionId, blockId, readBufferSize);
  }

  public ShuffleDataResult getShuffleData(
      String appId, Integer shuffleId, Integer partitionId, int partitionNumPerRange,
      int partitionNum, String storageType, long offset, int length) {
    refreshAppId(appId);

    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);
    Storage storage = storageManager.selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId));

    return storage.getOrCreateReadHandler(request).getShuffleData(offset, length);
  }

  public ShuffleIndexResult getShuffleIndex(
      String appId,
      Integer shuffleId,
      Integer partitionId,
      int partitionNumPerRange,
      int partitionNum) {
    refreshAppId(appId);
    String storageType = conf.getString(RssBaseConf.RSS_STORAGE_TYPE);
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);

    Storage storage = storageManager.selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId));
    return storage.getOrCreateReadHandler(request).getShuffleIndex();
  }

  public void checkResourceStatus() {
    try {
      Set<String> appNames = Sets.newHashSet(appIds.keySet());
      // remove applications which is timeout according to rss.server.app.expired.withoutHeartbeat
      for (String appId : appNames) {
        if (System.currentTimeMillis() - appIds.get(appId) > appExpiredWithoutHB) {
          LOG.info("Detect expired appId[" + appId + "] according "
              + "to rss.server.app.expired.withoutHeartbeat");
          expiredAppIdQueue.add(appId);
        }
      }
      ShuffleServerMetrics.gaugeAppNum.set(appIds.size());
    } catch (Exception e) {
      LOG.warn("Error happened in checkResourceStatus", e);
    }
  }

  @VisibleForTesting
  public void removeResources(String appId) {
    LOG.info("Start remove resource for appId[" + appId + "]");
    final long start = System.currentTimeMillis();
    final Map<Integer, Roaring64NavigableMap> shuffleToCachedBlockIds = cachedBlockIds.get(appId);
    appIds.remove(appId);
    partitionsToBlockIds.remove(appId);
    cachedBlockIds.remove(appId);
    commitCounts.remove(appId);
    commitLocks.remove(appId);
    shuffleBufferManager.removeBuffer(appId);
    shuffleFlushManager.removeResources(appId);
    if (shuffleToCachedBlockIds != null) {
      storageManager.removeResources(appId, shuffleToCachedBlockIds.keySet());
    }
    LOG.info("Finish remove resource for appId[" + appId + "] cost " + (System.currentTimeMillis() - start) + " ms");
  }

  public void refreshAppId(String appId) {
    appIds.put(appId, System.currentTimeMillis());
  }

  // check pre allocated buffer, release the memory if it expired
  private void preAllocatedBufferCheck() {
    try {
      long current = System.currentTimeMillis();
      List<Long> removeIds = Lists.newArrayList();
      for (PreAllocatedBufferInfo info : requireBufferIds.values()) {
        if (current - info.getTimestamp() > preAllocationExpired) {
          removeIds.add(info.getRequireId());
          shuffleBufferManager.releaseMemory(info.getRequireSize(), false, true);
        }
      }
      for (Long requireId : removeIds) {
        requireBufferIds.remove(requireId);
        LOG.info("Remove expired requireId " + requireId);
      }
    } catch (Exception e) {
      LOG.warn("Error happened in preAllocatedBufferCheck", e);
    }
  }

  public int getRequireBufferSize(long requireId) {
    PreAllocatedBufferInfo pabi = requireBufferIds.get(requireId);
    if (pabi == null) {
      return 0;
    }
    return pabi.getRequireSize();
  }

  @VisibleForTesting
  public Map<String, Long> getAppIds() {
    return appIds;
  }

  @VisibleForTesting
  Map<Long, PreAllocatedBufferInfo> getRequireBufferIds() {
    return requireBufferIds;
  }

  @VisibleForTesting
  public Map<String, Map<Integer, Roaring64NavigableMap[]>> getPartitionsToBlockIds() {
    return partitionsToBlockIds;
  }
}
