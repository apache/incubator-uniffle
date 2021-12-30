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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.server.ShuffleServerConf;
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
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    return localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
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

  public List<LocalStorage> getStorages() {
    return localStorages;
  }
}
