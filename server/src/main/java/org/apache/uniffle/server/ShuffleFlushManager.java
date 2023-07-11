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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION;

public class ShuffleFlushManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFlushManager.class);
  public static final AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);
  private final ShuffleServer shuffleServer;
  private final List<String> storageBasePaths;
  private final String storageType;
  private final int storageDataReplica;
  private final ShuffleServerConf shuffleServerConf;
  private Configuration hadoopConf;
  // appId -> shuffleId -> committed shuffle blockIds
  private Map<String, Map<Integer, Roaring64NavigableMap>> committedBlockIds = JavaUtils.newConcurrentMap();
  private final int retryMax;

  private final StorageManager storageManager;
  private final long pendingEventTimeoutSec;
  private FlushEventHandler eventHandler;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, ShuffleServer shuffleServer,
                             StorageManager storageManager) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;
    this.storageManager = storageManager;
    initHadoopConf();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE);
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);

    storageBasePaths = RssUtils.getConfiguredLocalDirs(shuffleServerConf);
    pendingEventTimeoutSec = shuffleServerConf.getLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
    eventHandler = new DefaultFlushEventHandler(shuffleServerConf, storageManager, this::processEvent);
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    eventHandler.handle(event);
  }

  public void processEvent(ShuffleDataFlushEvent event) {
    try {
      ShuffleServerMetrics.gaugeWriteHandler.inc();
      long start = System.currentTimeMillis();
      boolean writeSuccess = flushToFile(event);
      if (writeSuccess || event.getRetryTimes() > retryMax) {
        if (event.getRetryTimes() > retryMax) {
          LOG.error("Failed to write data for {} in {} times, shuffle data will be lost", event, retryMax);
          if (event.getUnderStorage() != null) {
            ShuffleServerMetrics.incStorageFailedCounter(event.getUnderStorage().getStorageHost());
          }
        }
        event.doCleanup();
        if (shuffleServer != null) {
          long duration = System.currentTimeMillis() - start;
          if (writeSuccess) {
            LOG.debug("Flush to file success in {} ms and release {} bytes", duration, event.getSize());
          } else {
            ShuffleServerMetrics.counterTotalFailedWrittenEventNum.inc();
            LOG.error("Flush to file for {} failed in {} ms and release {} bytes", event, duration, event.getSize());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception happened when flush data for " + event, e);
    } finally {
      ShuffleServerMetrics.gaugeWriteHandler.dec();
      ShuffleServerMetrics.gaugeEventQueueSize.dec();
    }
  }

  private boolean flushToFile(ShuffleDataFlushEvent event) {
    boolean writeSuccess = false;

    try {
      if (!event.isValid()) {
        LOG.warn("AppId {} was removed already, event {} should be dropped", event.getAppId(), event);
        return true;
      }

      List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
      if (blocks == null || blocks.isEmpty()) {
        LOG.info("There is no block to be flushed: {}", event);
        return true;
      }

      Storage storage = event.getUnderStorage();
      if (storage == null) {
        LOG.error("Storage selected is null and this should not happen. event: {}", event);
        return true;
      }

      if (event.isPended()
              && System.currentTimeMillis() - event.getStartPendingTime() > pendingEventTimeoutSec * 1000L) {
        ShuffleServerMetrics.counterTotalDroppedEventNum.inc();
        LOG.error("Flush event cannot be flushed for {} sec, the event {} is dropped",
            pendingEventTimeoutSec, event);
        return true;
      }

      if (!storage.canWrite()) {
        // todo: Could we add an interface supportPending for storageManager
        //       to unify following logic of multiple different storage managers
        if (event.getRetryTimes() <= retryMax) {
          if (event.isPended()) {
            LOG.error("Drop this event directly due to already having entered pending queue. event: {}", event);
            return true;
          }
          event.increaseRetryTimes();
          ShuffleServerMetrics.incStorageRetryCounter(storage.getStorageHost());
          event.markPended();
          eventHandler.handle(event);
        }
        return false;
      }

      String user = StringUtils.defaultString(
          shuffleServer.getShuffleTaskManager().getUserByAppId(event.getAppId()),
          StringUtils.EMPTY
      );
      int maxConcurrencyPerPartitionToWrite = getMaxConcurrencyPerPartitionWrite(event);
      CreateShuffleWriteHandlerRequest request = new CreateShuffleWriteHandlerRequest(
          storageType,
          event.getAppId(),
          event.getShuffleId(),
          event.getStartPartition(),
          event.getEndPartition(),
          storageBasePaths.toArray(new String[storageBasePaths.size()]),
          getShuffleServerId(),
          hadoopConf,
          storageDataReplica,
          user,
          maxConcurrencyPerPartitionToWrite);
      ShuffleWriteHandler handler = storage.getOrCreateWriteHandler(request);
      writeSuccess = storageManager.write(storage, handler, event);
      if (writeSuccess) {
        updateCommittedBlockIds(event.getAppId(), event.getShuffleId(), blocks);
        ShuffleServerMetrics.incStorageSuccessCounter(storage.getStorageHost());
      } else if (event.getRetryTimes() <= retryMax) {
        if (event.isPended()) {
          LOG.error("Drop this event directly due to already having entered pending queue. event: {}", event);
        }
        event.increaseRetryTimes();
        ShuffleServerMetrics.incStorageRetryCounter(storage.getStorageHost());
        event.markPended();
        eventHandler.handle(event);
      }
    } catch (Throwable throwable) {
      // just log the error, don't throw the exception and stop the flush thread
      LOG.error("Exception happened when process flush shuffle data for {}", event, throwable);
      event.increaseRetryTimes();
    }
    return writeSuccess;
  }

  private int getMaxConcurrencyPerPartitionWrite(ShuffleDataFlushEvent event) {
    ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(event.getAppId());
    // For some tests.
    if (taskInfo == null) {
      LOG.warn("Should not happen that shuffle task info of {} is null.", event.getAppId());
      return shuffleServerConf.get(SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION);
    }
    return taskInfo.getMaxConcurrencyPerPartitionToWrite();
  }

  private String getShuffleServerId() {
    return shuffleServerConf.getString(ShuffleServerConf.SHUFFLE_SERVER_ID, "shuffleServerId");
  }

  private void updateCommittedBlockIds(String appId, int shuffleId, List<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return;
    }
    committedBlockIds.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, Roaring64NavigableMap> shuffleToBlockIds = committedBlockIds.get(appId);
    shuffleToBlockIds.computeIfAbsent(shuffleId, key -> Roaring64NavigableMap.bitmapOf());
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
    committedBlockIds.remove(appId);
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
    return eventHandler.getEventNumInFlush();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void removeResourcesOfShuffleId(String appId, Collection<Integer> shuffleIds) {
    Optional.ofNullable(committedBlockIds.get(appId))
        .ifPresent(shuffleIdToBlockIds -> shuffleIds.forEach(shuffleIdToBlockIds::remove));
  }

  public ShuffleDataDistributionType getDataDistributionType(String appId) {
    return shuffleServer.getShuffleTaskManager().getDataDistributionType(appId);
  }
}
