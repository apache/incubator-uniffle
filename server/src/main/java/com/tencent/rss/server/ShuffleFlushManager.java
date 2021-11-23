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
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleFlushManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFlushManager.class);
  public static AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);
  private final ShuffleServer shuffleServer;
  private final BlockingQueue<ShuffleDataFlushEvent> flushQueue = Queues.newLinkedBlockingQueue();
  private final ThreadPoolExecutor threadPoolExecutor;
  private final String[] storageBasePaths;
  private final String shuffleServerId;
  private final String storageType;
  private final int storageDataReplica;
  private final ShuffleServerConf shuffleServerConf;
  private Configuration hadoopConf;
  // appId -> shuffleId -> partitionId -> handlers
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> handlers = Maps.newConcurrentMap();
  // appId -> shuffleId -> committed shuffle blockIds
  private Map<String, Map<Integer, Roaring64NavigableMap>> committedBlockIds = Maps.newConcurrentMap();
  private Runnable processEventThread;
  private final int retryMax;
  private final long writeSlowThreshold;
  private final long eventSizeThresholdL1;
  private final long eventSizeThresholdL2;
  private final long eventSizeThresholdL3;
  private final MultiStorageManager multiStorageManager;
  private final BlockingQueue<PendingShuffleFlushEvent> pendingEvents = Queues.newLinkedBlockingQueue();
  private final long pendingEventTimeoutSec;
  private final boolean useMultiStorage;
  private int processPendingEventIndex = 0;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, String shuffleServerId, ShuffleServer shuffleServer,
      MultiStorageManager multiStorageManager) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;
    this.multiStorageManager = multiStorageManager;
    initHadoopConf();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE);
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);
    writeSlowThreshold = shuffleServerConf.getLong(ShuffleServerConf.SERVER_WRITE_SLOW_THRESHOLD);
    eventSizeThresholdL1 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L1);
    eventSizeThresholdL2 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L2);
    eventSizeThresholdL3 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L3);
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_SIZE);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue);
    storageBasePaths = shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH).split(",");
    pendingEventTimeoutSec = shuffleServerConf.getLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
    useMultiStorage = shuffleServerConf.getBoolean(ShuffleServerConf.USE_MULTI_STORAGE);
    // the thread for flush data
    processEventThread = () -> {
      while (true) {
        try {
          ShuffleDataFlushEvent event = flushQueue.take();
          threadPoolExecutor.execute(() -> {
            ShuffleServerMetrics.gaugeEventQueueSize.set(flushQueue.size());
            ShuffleServerMetrics.gaugeWriteHandler.inc();
            flushToFile(event);
            ShuffleServerMetrics.gaugeWriteHandler.dec();
          });
        } catch (Exception e) {
          LOG.error("Exception happened when process event.", e);
        }
      }
    };
    new Thread(processEventThread).start();
    // todo: extract a class named Service, and support stop method
    if (useMultiStorage) {
      Thread thread = new Thread("PendingEventProcessThread") {
        @Override
        public void run() {
          for (;;) {
            try {
              processPendingEvents();
              processPendingEventIndex = (processPendingEventIndex + 1) % 1000;
              if (processPendingEventIndex == 0) {
                // todo: get sleep interval from configuration
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
              }
            } catch (Exception e) {
              LOG.error(getName() + " happened exception: ", e);
            }
          }
        }
      };
      thread.setDaemon(true);
      thread.start();
    }
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    flushQueue.offer(event);
  }

  private void flushToFile(ShuffleDataFlushEvent event) {

    if (useMultiStorage && !multiStorageManager.canWrite(event)) {
      addPendingEvents(event);
      return;
    }

    long start = System.currentTimeMillis();
    List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
    boolean writeSuccess = false;
    try {
      if (blocks == null || blocks.isEmpty()) {
        LOG.info("There is no block to be flushed: " + event);
      } else if (!event.isValid()) {
        LOG.warn("AppId {} was removed already, event {} should be dropped", event.getAppId(), event);
      } else {
        ShuffleWriteHandler handler = getHandler(event);
        int retry = 0;
        while (!writeSuccess) {
          if (retry > retryMax) {
            LOG.error("Failed to write data for " + event + " in " + retryMax + " times, shuffle data will be lost");
            break;
          }
          if (!event.isValid()) {
            LOG.warn("AppId {} was removed already, event {} should be dropped, may leak one handler",
                event.getAppId(), event);
            break;
          }
          ReadWriteLock lock = null;
          if (useMultiStorage) {
            multiStorageManager.createMetadataIfNotExist(event);
            lock = multiStorageManager.getForceUploadLock(event);
            if (lock == null) {
              LOG.warn("Shuffle metadata of event {}/{} has been removed, ignore this flush event",
                  event.getAppId(), event.getShuffleId());
              return;
            }
            lock.readLock().lock();
          }
          try {
            long startWrite = System.currentTimeMillis();
            handler.write(blocks);
            if (useMultiStorage) {
              multiStorageManager.updateWriteEvent(event);
            }
            long writeTime = System.currentTimeMillis() - startWrite;
            ShuffleServerMetrics.counterTotalWriteTime.inc(writeTime);
            ShuffleServerMetrics.counterWriteTotal.inc();
            if (writeTime > writeSlowThreshold) {
              ShuffleServerMetrics.counterWriteSlow.inc();
            }
            updateCommittedBlockIds(event.getAppId(), event.getShuffleId(), blocks);
            writeSuccess = true;
            ShuffleServerMetrics.counterTotalWriteDataSize.inc(event.getSize());
            ShuffleServerMetrics.counterTotalWriteBlockSize.inc(event.getShuffleBlocks().size());
            if (event.getSize() < eventSizeThresholdL1) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel1.inc();
            } else if (event.getSize() < eventSizeThresholdL2) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel2.inc();
            } else if (event.getSize() < eventSizeThresholdL3) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel3.inc();
            } else {
              ShuffleServerMetrics.counterEventSizeThresholdLevel4.inc();
            }
          } catch (Exception e) {
            LOG.warn("Exception happened when write data for " + event + ", try again", e);
            ShuffleServerMetrics.counterWriteException.inc();
            Thread.sleep(1000);
          } finally {
            if (useMultiStorage) {
              lock.readLock().unlock();
            }
          }
          retry++;
        }
      }
    } catch (Exception e) {
      // just log the error, don't throw the exception and stop the flush thread
      LOG.error("Exception happened when process flush shuffle data for " + event, e);
    } finally {
      if (shuffleServer != null) {
        shuffleServer.getShuffleBufferManager().releaseMemory(event.getSize(), true, false);
        long duration = System.currentTimeMillis() - start;
        if (writeSuccess) {
          LOG.debug("Flush to file success in " + duration + " ms and release " + event.getSize() + " bytes");
        } else {
          LOG.error("Flush to file for " + event + " failed in "
              + duration + " ms and release " + event.getSize() + " bytes");
        }
      }
    }
  }

  private synchronized ShuffleWriteHandler getHandler(ShuffleDataFlushEvent event) throws Exception {
    handlers.putIfAbsent(event.getAppId(), Maps.newConcurrentMap());
    Map<Integer, RangeMap<Integer, ShuffleWriteHandler>> shuffleIdToHandlers = handlers.get(event.getAppId());
    shuffleIdToHandlers.putIfAbsent(event.getShuffleId(), TreeRangeMap.create());
    RangeMap<Integer, ShuffleWriteHandler> eventIdRangeMap = shuffleIdToHandlers.get(event.getShuffleId());
    if (eventIdRangeMap.get(event.getStartPartition()) == null) {
      eventIdRangeMap.put(
          Range.closed(event.getStartPartition(), event.getEndPartition()),
          ShuffleHandlerFactory
              .getInstance()
              .createShuffleWriteHandler(
                  new CreateShuffleWriteHandlerRequest(
                      storageType,
                      event.getAppId(),
                      event.getShuffleId(),
                      event.getStartPartition(),
                      event.getEndPartition(),
                      storageBasePaths,
                      shuffleServerId,
                      hadoopConf,
                      storageDataReplica)));
    }
    return eventIdRangeMap.get(event.getStartPartition());
  }

  private void updateCommittedBlockIds(String appId, int shuffleId, List<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return;
    }
    if (!committedBlockIds.containsKey(appId)) {
      committedBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    }
    Map<Integer, Roaring64NavigableMap> shuffleToBlockIds = committedBlockIds.get(appId);
    shuffleToBlockIds.putIfAbsent(shuffleId, Roaring64NavigableMap.bitmapOf());
    Roaring64NavigableMap bitmap = shuffleToBlockIds.get(shuffleId);
    synchronized (bitmap) {
      for (ShufflePartitionedBlock spb : blocks) {
        bitmap.addLong(spb.getBlockId());
      }
    }
  }

  public Roaring64NavigableMap getCommittedBlockIds(String appId, Integer shuffleId) {
    Map<Integer, Roaring64NavigableMap> shuffleIdToBlockIds = committedBlockIds.get(appId);
    if (shuffleIdToBlockIds == null) {
      LOG.warn("Unexpected value when getCommittedBlockIds for appId[" + appId + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    Roaring64NavigableMap blockIds = shuffleIdToBlockIds.get(shuffleId);
    if (blockIds == null) {
      LOG.warn("Unexpected value when getCommittedBlockIds for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    return blockIds;
  }

  public void removeResources(String appId) {
    handlers.remove(appId);
    committedBlockIds.remove(appId);
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest(storageType, hadoopConf));
    deleteHandler.delete(storageBasePaths, appId);
  }

  protected void initHadoopConf() {
    hadoopConf = new Configuration();
    for (String key : shuffleServerConf.getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = shuffleServerConf.getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        LOG.info("Update hadoop configuration:" + hadoopKey + "=" + value);
        hadoopConf.set(hadoopKey, value);
      }
    }
  }

  public int getEventNumInFlush() {
    return flushQueue.size();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> getHandlers() {
    return handlers;
  }

  @VisibleForTesting
  void processPendingEvents() throws Exception {
    if (!useMultiStorage) {
      return;
    }
    PendingShuffleFlushEvent event = pendingEvents.take();
    DiskItem item = multiStorageManager.getDiskItem(event.getEvent());
    if (System.currentTimeMillis() - event.getCreateTimeStamp() > pendingEventTimeoutSec * 1000L) {
      ShuffleServerMetrics.counterTotalDroppedEventNum.inc();
      if (shuffleServer != null) {
        shuffleServer.getShuffleBufferManager().releaseMemory(
            event.getEvent().getSize(), true, false);
      }
      LOG.error("Flush event cannot be flushed for {} sec, the event {} is dropped",
          pendingEventTimeoutSec, event.getEvent());
      return;
    }
    if (item.canWrite()) {
      addToFlushQueue(event.getEvent());
      return;
    }
    addPendingEventsInternal(event);
  }

  @VisibleForTesting
  void addPendingEvents(ShuffleDataFlushEvent event) {
    addPendingEventsInternal(new PendingShuffleFlushEvent(event));
  }

  @VisibleForTesting
  int getPendingEventsSize() {
    return pendingEvents.size();
  }

  private void addPendingEventsInternal(PendingShuffleFlushEvent event) {
    boolean pendingEventsResult = pendingEvents.offer(event);
    ShuffleDataFlushEvent flushEvent = event.getEvent();
    if (!pendingEventsResult) {
      LOG.error("Post pendingEvent queue fail!! App: " + flushEvent.getAppId() + " Shuffle "
          + flushEvent.getShuffleId() + " Partition " + flushEvent.getStartPartition());
    }
  }

  private class PendingShuffleFlushEvent {
    private final ShuffleDataFlushEvent event;
    private final long createTimeStamp = System.currentTimeMillis();

    PendingShuffleFlushEvent(ShuffleDataFlushEvent event) {
      this.event = event;
    }

    public ShuffleDataFlushEvent getEvent() {
      return event;
    }

    public long getCreateTimeStamp() {
      return createTimeStamp;
    }
  }
}
