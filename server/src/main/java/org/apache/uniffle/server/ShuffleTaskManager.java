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

package org.apache.uniffle.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.commons.collections.CollectionUtils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final ShuffleFlushManager shuffleFlushManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ScheduledExecutorService expiredAppCleanupExecutorService;
  private final ScheduledExecutorService leakShuffleDataCheckExecutorService;
  private final StorageManager storageManager;
  private AtomicLong requireBufferId = new AtomicLong(0);
  private ShuffleServerConf conf;
  private long appExpiredWithoutHB;
  private long preAllocationExpired;
  private long commitCheckIntervalMax;
  private long leakShuffleDataCheckInterval;
  // appId -> shuffleId -> blockIds to avoid too many appId
  // store taskAttemptId info to filter speculation task
  // Roaring64NavigableMap instance will cost much memory,
  // merge different blockId of partition to one bitmap can reduce memory cost,
  // but when get blockId, performance will degrade a little which can be optimized by client configuration
  private Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds;
  private ShuffleBufferManager shuffleBufferManager;
  private Map<String, ShuffleTaskInfo> shuffleTaskInfos = Maps.newConcurrentMap();
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds = Maps.newConcurrentMap();
  private Runnable clearResourceThread;
  private BlockingQueue<PurgeEvent> expiredAppIdQueue = Queues.newLinkedBlockingQueue();
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
    this.leakShuffleDataCheckInterval = conf.getLong(ShuffleServerConf.SERVER_LEAK_SHUFFLE_DATA_CHECK_INTERVAL);
    // the thread for checking application status
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("checkResource-%d"));
    scheduledExecutorService.scheduleAtFixedRate(
        () -> preAllocatedBufferCheck(), preAllocationExpired / 2,
        preAllocationExpired / 2, TimeUnit.MILLISECONDS);
    this.expiredAppCleanupExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("expiredAppCleaner"));
    expiredAppCleanupExecutorService.scheduleAtFixedRate(
        () -> checkResourceStatus(), appExpiredWithoutHB / 2,
        appExpiredWithoutHB / 2, TimeUnit.MILLISECONDS);
    this.leakShuffleDataCheckExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("leakShuffleDataChecker"));
    leakShuffleDataCheckExecutorService.scheduleAtFixedRate(
        () -> checkLeakShuffleData(), leakShuffleDataCheckInterval,
            leakShuffleDataCheckInterval, TimeUnit.MILLISECONDS);
    // the thread for clear expired resources
    clearResourceThread = () -> {
      while (true) {
        try {
          PurgeEvent event = expiredAppIdQueue.take();
          if (event instanceof AppPurgeEvent) {
            removeResources(event.getAppId());
          }
          if (event instanceof ShufflePurgeEvent) {
            removeResourcesByShuffleIds(event.getAppId(), ((ShufflePurgeEvent) event).getShuffleIds());
          }
        } catch (Exception e) {
          LOG.error("Exception happened when clear resource for expired application", e);
        }
      }
    };
    Thread thread = new Thread(clearResourceThread);
    thread.setName("clearResourceThread");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Only for test
   */
  @VisibleForTesting
  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user) {
    return registerShuffle(
        appId,
        shuffleId,
        partitionRanges,
        remoteStorageInfo,
        user,
        ShuffleDataDistributionType.NORMAL
    );
  }

  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistType) {
    refreshAppId(appId);
    shuffleTaskInfos.get(appId).setUser(user);
    shuffleTaskInfos.get(appId).setDataDistType(dataDistType);
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    for (PartitionRange partitionRange : partitionRanges) {
      shuffleBufferManager.registerBuffer(appId, shuffleId, partitionRange.getStart(), partitionRange.getEnd());
    }
    if (!remoteStorageInfo.isEmpty()) {
      storageManager.registerRemoteStorage(appId, remoteStorageInfo);
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
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo());
    Object lock = shuffleTaskInfo.getCommitLocks().computeIfAbsent(shuffleId, x -> new Object());
    synchronized (lock) {
      long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      synchronized (cachedBlockIds) {
        cloneBlockIds = RssUtils.cloneBitMap(cachedBlockIds);
      }
      long expectedCommitted = cloneBlockIds.getLongCardinality();
      shuffleBufferManager.commitShuffleTask(appId, shuffleId);
      Roaring64NavigableMap committedBlockIds;
      Roaring64NavigableMap cloneCommittedBlockIds;
      long checkInterval = 1000L;
      while (true) {
        committedBlockIds = shuffleFlushManager.getCommittedBlockIds(appId, shuffleId);
        synchronized (committedBlockIds) {
          cloneCommittedBlockIds = RssUtils.cloneBitMap(committedBlockIds);
        }
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
    if (shuffleIdToPartitions == null) {
      throw new RuntimeException("appId[" + appId  + "] is expired!");
    }
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

  public int updateAndGetCommitCount(String appId, int shuffleId) {
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo());
    AtomicInteger commitNum = shuffleTaskInfo.getCommitCounts()
        .computeIfAbsent(shuffleId, x -> new AtomicInteger(0));
    return commitNum.incrementAndGet();
  }

  public void updateCachedBlockIds(String appId, int shuffleId, ShufflePartitionedBlock[] spbs) {
    if (spbs == null || spbs.length == 0) {
      return;
    }
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo());
    Roaring64NavigableMap bitmap = shuffleTaskInfo.getCachedBlockIds()
        .computeIfAbsent(shuffleId, x -> Roaring64NavigableMap.bitmapOf());
    synchronized (bitmap) {
      for (ShufflePartitionedBlock spb : spbs) {
        bitmap.addLong(spb.getBlockId());
      }
    }
  }

  public Roaring64NavigableMap getCachedBlockIds(String appId, int shuffleId) {
    Map<Integer, Roaring64NavigableMap> shuffleIdToBlockIds = shuffleTaskInfos
        .getOrDefault(appId, new ShuffleTaskInfo()).getCachedBlockIds();
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

  public byte[] getFinishedBlockIds(String appId, Integer shuffleId, Set<Integer> partitions) throws IOException {
    refreshAppId(appId);
    for (int partitionId : partitions) {
      Storage storage = storageManager.selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId));
      // update shuffle's timestamp that was recently read.
      storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
    }
    Map<Integer, Roaring64NavigableMap[]> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      return null;
    }

    Roaring64NavigableMap[] blockIds = shuffleIdToPartitions.get(shuffleId);
    if (blockIds == null) {
      return new byte[]{};
    }
    Map<Integer, Set<Integer>> bitmapIndexToPartitions = Maps.newHashMap();
    for (int partitionId : partitions) {
      int bitmapIndex = partitionId % blockIds.length;
      if (bitmapIndexToPartitions.containsKey(bitmapIndex)) {
        bitmapIndexToPartitions.get(bitmapIndex).add(partitionId);
      } else {
        HashSet<Integer> newHashSet = Sets.newHashSet(partitionId);
        bitmapIndexToPartitions.put(bitmapIndex, newHashSet);
      }
    }

    Roaring64NavigableMap res = Roaring64NavigableMap.bitmapOf();
    for (Map.Entry<Integer, Set<Integer>> entry : bitmapIndexToPartitions.entrySet()) {
      Set<Integer> requestPartitions = entry.getValue();
      Roaring64NavigableMap bitmap = blockIds[entry.getKey()];
      getBlockIdsByPartitionId(requestPartitions, bitmap, res);
    }
    return RssUtils.serializeBitMap(res);
  }


  // filter the specific partition blockId in the bitmap to the resultBitmap
  protected Roaring64NavigableMap getBlockIdsByPartitionId(Set<Integer> requestPartitions,
      Roaring64NavigableMap bitmap, Roaring64NavigableMap resultBitmap) {
    LongIterator iter = bitmap.getLongIterator();
    long mask = (1L << Constants.PARTITION_ID_MAX_LENGTH) - 1;
    while (iter.hasNext()) {
      long blockId = iter.next();
      int partitionId = Math.toIntExact((blockId >> Constants.TASK_ATTEMPT_ID_MAX_LENGTH) & mask);
      if (requestPartitions.contains(partitionId)) {
        resultBitmap.addLong(blockId);
      }
    }
    return resultBitmap;
  }

  public ShuffleDataResult getInMemoryShuffleData(
      String appId, Integer shuffleId, Integer partitionId, long blockId, int readBufferSize,
      Roaring64NavigableMap expectedTaskIds, List<Long> expectedBlockIdRange) {
    return shuffleBufferManager.getShuffleData(appId,
        shuffleId, partitionId, blockId, readBufferSize, expectedTaskIds, expectedBlockIdRange);
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
      Set<String> appNames = Sets.newHashSet(shuffleTaskInfos.keySet());
      // remove applications which is timeout according to rss.server.app.expired.withoutHeartbeat
      for (String appId : appNames) {
        if (System.currentTimeMillis() - shuffleTaskInfos.get(appId).getCurrentTimes() > appExpiredWithoutHB) {
          LOG.info("Detect expired appId[" + appId + "] according "
              + "to rss.server.app.expired.withoutHeartbeat");
          expiredAppIdQueue.add(new AppPurgeEvent(appId, getUserByAppId(appId)));
        }
      }
      ShuffleServerMetrics.gaugeAppNum.set(shuffleTaskInfos.size());
    } catch (Exception e) {
      LOG.warn("Error happened in checkResourceStatus", e);
    }
  }

  /**
   * Clear up the partial resources of shuffleIds of App.
   * @param appId
   * @param shuffleIds
   */
  public void removeResourcesByShuffleIds(String appId, List<Integer> shuffleIds) {
    if (CollectionUtils.isEmpty(shuffleIds)) {
      return;
    }

    LOG.info("Start remove resource for appId[{}], shuffleIds[{}]", appId, shuffleIds);
    final long start = System.currentTimeMillis();
    final ShuffleTaskInfo taskInfo = shuffleTaskInfos.get(appId);
    if (taskInfo != null) {
      for (Integer shuffleId : shuffleIds) {
        Optional.ofNullable(taskInfo).ifPresent(x -> x.getCachedBlockIds().remove(shuffleId));
        Optional.ofNullable(taskInfo).ifPresent(x -> x.getCommitCounts().remove(shuffleId));
        Optional.ofNullable(taskInfo).ifPresent(x -> x.getCommitLocks().remove(shuffleId));
      }
    }
    Optional.ofNullable(partitionsToBlockIds.get(appId)).ifPresent(x -> {
      for (Integer shuffleId : shuffleIds) {
        x.remove(shuffleId);
      }
    });
    shuffleBufferManager.removeBufferByShuffleId(appId, shuffleIds.toArray(new Integer[0]));
    for (Integer shuffleId : shuffleIds) {
      shuffleFlushManager.removeResourcesOfShuffleId(appId, shuffleId);
    }
    storageManager.removeResources(
        new ShufflePurgeEvent(appId, getUserByAppId(appId), shuffleIds)
    );
    LOG.info("Finish remove resource for appId[{}], shuffleIds[{}], cost[{}]",
        appId, shuffleIds, System.currentTimeMillis() - start);
  }

  public void checkLeakShuffleData() {
    LOG.info("Start check leak shuffle data");
    try {
      Set<String> appIds = Sets.newHashSet(shuffleTaskInfos.keySet());
      storageManager.checkAndClearLeakedShuffleData(appIds);
      LOG.info("Finish check leak shuffle data");
    } catch (Exception e) {
      LOG.warn("Error happened in checkLeakShuffleData", e);
    }
  }

  @VisibleForTesting
  public void removeResources(String appId) {
    LOG.info("Start remove resource for appId[" + appId + "]");
    final long start = System.currentTimeMillis();
    final Map<Integer, Roaring64NavigableMap> shuffleToCachedBlockIds = shuffleTaskInfos.get(appId).getCachedBlockIds();
    partitionsToBlockIds.remove(appId);
    shuffleBufferManager.removeBuffer(appId);
    shuffleFlushManager.removeResources(appId);
    if (!shuffleToCachedBlockIds.isEmpty()) {
      storageManager.removeResources(
          new AppPurgeEvent(appId, getUserByAppId(appId), new ArrayList<>(shuffleToCachedBlockIds.keySet()))
      );
    }
    shuffleTaskInfos.remove(appId);
    LOG.info("Finish remove resource for appId[" + appId + "] cost " + (System.currentTimeMillis() - start) + " ms");
  }

  public void refreshAppId(String appId) {
    shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo()).setCurrentTimes(System.currentTimeMillis());
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

  public String getUserByAppId(String appId) {
    return shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo()).getUser();
  }

  @VisibleForTesting
  public Set<String> getAppIds() {
    return shuffleTaskInfos.keySet();
  }

  @VisibleForTesting
  Map<Long, PreAllocatedBufferInfo> getRequireBufferIds() {
    return requireBufferIds;
  }

  @VisibleForTesting
  public Map<String, Map<Integer, Roaring64NavigableMap[]>> getPartitionsToBlockIds() {
    return partitionsToBlockIds;
  }

  public void removeShuffleDataAsync(String appId, int shuffleId) {
    expiredAppIdQueue.add(new ShufflePurgeEvent(appId, getUserByAppId(appId), Arrays.asList(shuffleId)));
  }

  @VisibleForTesting
  void removeShuffleDataSync(String appId, int shuffleId) {
    removeResourcesByShuffleIds(appId, Arrays.asList(shuffleId));
  }

  public ShuffleDataDistributionType getDataDistributionType(String appId) {
    return shuffleTaskInfos.get(appId).getDataDistType();
  }
}
