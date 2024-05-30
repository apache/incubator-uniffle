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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.flush.EventDiscardException;
import org.apache.uniffle.server.flush.EventInvalidException;
import org.apache.uniffle.server.flush.EventRetryException;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.LocalStorage;
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
  private Map<String, Map<Integer, BlockIdSet>> committedBlockIds = JavaUtils.newConcurrentMap();
  private final int retryMax;

  private final StorageManager storageManager;
  private final long pendingEventTimeoutSec;
  private FlushEventHandler eventHandler;

  public ShuffleFlushManager(
      ShuffleServerConf shuffleServerConf,
      ShuffleServer shuffleServer,
      StorageManager storageManager) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;
    this.storageManager = storageManager;
    initHadoopConf();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE).name();
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);

    storageBasePaths = RssUtils.getConfiguredLocalDirs(shuffleServerConf);
    pendingEventTimeoutSec = shuffleServerConf.getLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
    eventHandler =
        new DefaultFlushEventHandler(
            shuffleServerConf, storageManager, shuffleServer, this::processFlushEvent);
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    eventHandler.handle(event);
  }

  /**
   * The method to handle flush event to flush blocks into persistent storage. And we will not
   * change any internal state for event, that means the event is read-only for this processing.
   *
   * <p>Only the blocks are flushed successfully, it can return directly, otherwise it should always
   * throw dedicated exception.
   *
   * @param event
   * @throws Exception
   */
  public void processFlushEvent(ShuffleDataFlushEvent event) throws Exception {
    try {
      ShuffleServerMetrics.gaugeWriteHandler.inc();

      if (!event.isValid()) {
        LOG.warn(
            "AppId {} was removed already, event:{} should be dropped", event.getAppId(), event);
        // we should catch this to avoid cleaning up duplicate.
        throw new EventInvalidException();
      }

      if (reachRetryMax(event)) {
        LOG.error("The event:{} has been reached to max retry times, it will be dropped.", event);
        throw new EventDiscardException();
      }

      List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
      if (CollectionUtils.isEmpty(blocks)) {
        LOG.info("There is no block to be flushed: {}", event);
        return;
      }

      Storage storage = event.getUnderStorage();
      if (storage == null) {
        LOG.error("Storage selected is null and this should not happen. event: {}", event);
        throw new EventDiscardException();
      }

      if (event.isPended()
          && System.currentTimeMillis() - event.getStartPendingTime()
              > pendingEventTimeoutSec * 1000L) {
        LOG.error(
            "Flush event cannot be flushed for {} sec, the event {} is dropped",
            pendingEventTimeoutSec,
            event);
        throw new EventDiscardException();
      }

      if (!storage.canWrite()) {
        LOG.error(
            "The event: {} is limited to flush due to storage:{} can't write", event, storage);
        throw new EventRetryException();
      }

      String user =
          StringUtils.defaultString(
              shuffleServer.getShuffleTaskManager().getUserByAppId(event.getAppId()),
              StringUtils.EMPTY);
      int maxConcurrencyPerPartitionToWrite = getMaxConcurrencyPerPartitionWrite(event);
      CreateShuffleWriteHandlerRequest request =
          new CreateShuffleWriteHandlerRequest(
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
      boolean writeSuccess = storageManager.write(storage, handler, event);
      if (!writeSuccess) {
        throw new EventRetryException();
      }

      // update some metrics for shuffle task
      updateCommittedBlockIds(event.getAppId(), event.getShuffleId(), event.getShuffleBlocks());
      ShuffleTaskInfo shuffleTaskInfo =
          shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(event.getAppId());
      if (null != shuffleTaskInfo) {
        String storageHost = event.getUnderStorage().getStorageHost();
        if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
          shuffleTaskInfo.addOnLocalFileDataSize(event.getSize());
        } else {
          shuffleTaskInfo.addOnHadoopDataSize(event.getSize());
        }
      }
    } finally {
      ShuffleServerMetrics.gaugeWriteHandler.dec();
    }
  }

  private boolean reachRetryMax(ShuffleDataFlushEvent event) {
    return event.getRetryTimes() > retryMax;
  }

  private int getMaxConcurrencyPerPartitionWrite(ShuffleDataFlushEvent event) {
    ShuffleTaskInfo taskInfo =
        shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(event.getAppId());
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

  private void updateCommittedBlockIds(
      String appId, int shuffleId, List<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return;
    }
    committedBlockIds.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, BlockIdSet> shuffleToBlockIds = committedBlockIds.get(appId);
    shuffleToBlockIds.computeIfAbsent(shuffleId, key -> BlockIdSet.empty());
    BlockIdSet blockIds = shuffleToBlockIds.get(shuffleId);
    blockIds.addAll(blocks.stream().mapToLong(ShufflePartitionedBlock::getBlockId));
  }

  public BlockIdSet getCommittedBlockIds(String appId, Integer shuffleId) {
    Map<Integer, BlockIdSet> shuffleIdToBlockIds = committedBlockIds.get(appId);
    if (shuffleIdToBlockIds == null) {
      LOG.warn("Unexpected value when getCommittedBlockIds for appId[" + appId + "]");
      return BlockIdSet.empty();
    }
    BlockIdSet blockIds = shuffleIdToBlockIds.get(shuffleId);
    if (blockIds == null) {
      LOG.warn(
          "Unexpected value when getCommittedBlockIds for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "]");
      return BlockIdSet.empty();
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

  @VisibleForTesting
  public FlushEventHandler getEventHandler() {
    return eventHandler;
  }
}
