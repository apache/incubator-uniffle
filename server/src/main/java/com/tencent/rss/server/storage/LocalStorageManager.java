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

package com.tencent.rss.server.storage;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.server.Checker;
import com.tencent.rss.server.LocalStorageChecker;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.ShuffleServerMetrics;
import com.tencent.rss.storage.common.LocalStorage;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.StorageType;

public class LocalStorageManager extends SingleStorageManager {

  private final List<LocalStorage> localStorages = Lists.newArrayList();
  private final String[] storageBasePaths;
  private final LocalStorageChecker checker;
  private List<LocalStorage> unCorruptedStorages = Lists.newArrayList();
  private final Set<String> corruptedStorages = Sets.newConcurrentHashSet();

  LocalStorageManager(ShuffleServerConf conf) {
    super(conf);
    String storageBasePathStr = conf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH);
    if (StringUtils.isEmpty(storageBasePathStr)) {
      throw new IllegalArgumentException("Base path dirs must not be empty");
    }
    storageBasePaths = storageBasePathStr.split(",");
    long shuffleExpiredTimeoutMs = conf.get(ShuffleServerConf.SHUFFLE_EXPIRED_TIMEOUT_MS);
    long capacity = conf.getSizeAsBytes(ShuffleServerConf.DISK_CAPACITY);
    double highWaterMarkOfWrite = conf.get(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE);
    double lowWaterMarkOfWrite = conf.get(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE);
    if (highWaterMarkOfWrite < lowWaterMarkOfWrite) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite");
    }
    for (String storagePath : storageBasePaths) {
      localStorages.add(LocalStorage.newBuilder()
          .basePath(storagePath)
          .capacity(capacity)
          .lowWaterMarkOfWrite(lowWaterMarkOfWrite)
          .highWaterMarkOfWrite(highWaterMarkOfWrite)
          .shuffleExpiredTimeoutMs(shuffleExpiredTimeoutMs)
          .build());
    }
    this.checker = new LocalStorageChecker(conf, localStorages);
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    LocalStorage storage = localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
    if (storage.containsWriteHandler(event.getAppId(), event.getShuffleId(), event.getStartPartition())
        && storage.isCorrupted()) {
      throw new RuntimeException("storage " + storage.getBasePath() + " is corrupted");
    }
    if (storage.isCorrupted()) {
      storage = getRepairedStorage(event.getAppId(), event.getShuffleId(), event.getStartPartition());
    }
    return storage;
  }


  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {

    LocalStorage storage = localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
    if (storage.isCorrupted()) {
      storage = getRepairedStorage(event.getAppId(), event.getShuffleId(), event.getStartPartition());
    }
    return storage;
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalLocalFileWriteDataSize.inc(event.getSize());
  }

  @Override
  public Checker getStorageChecker() {
    return checker;
  }

  @Override
  public void removeResources(String appId, Set<Integer> shuffleSet) {
    for (LocalStorage storage : localStorages) {
      for (Integer shuffleId : shuffleSet) {
        storage.removeHandlers(appId);
        storage.removeResources(RssUtils.generateShuffleKey(appId, shuffleId));
      }
    }
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(
            new CreateShuffleDeleteHandlerRequest(StorageType.LOCALFILE.name(), new Configuration()));
    deleteHandler.delete(storageBasePaths, appId);
  }

  @Override
  public void registerRemoteStorage(String appId, String remoteStorage) {
    // ignore
  }

  void repair() {
    boolean hasNewCorruptedStorage = false;
    for (LocalStorage storage : localStorages) {
      if (storage.isCorrupted() && !corruptedStorages.contains(storage.getBasePath())) {
        hasNewCorruptedStorage = true;
        corruptedStorages.add(storage.getBasePath());
      }
    }
    if (hasNewCorruptedStorage) {
      List<LocalStorage> healthyStorages = Lists.newArrayList();
      for (LocalStorage storage : localStorages) {
        if (!storage.isCorrupted()) {
          healthyStorages.add(storage);
        }
      }
      unCorruptedStorages = healthyStorages;
    }
  }

  private synchronized LocalStorage getRepairedStorage(String appId, int shuffleId, int partitionId) {
    repair();
    if (unCorruptedStorages.isEmpty()) {
      throw new RuntimeException("No enough storages");
    }
    return  unCorruptedStorages.get(ShuffleStorageUtils.getStorageIndex(
        unCorruptedStorages.size(),
        appId,
        shuffleId,
        partitionId));
  }

  public List<LocalStorage> getStorages() {
    return localStorages;
  }
}
