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

import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.server.Checker;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.ShuffleServerMetrics;
import com.tencent.rss.storage.common.HdfsStorage;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.util.StorageType;

public class HdfsStorageManager extends SingleStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorageManager.class);

  private final Configuration hadoopConf;
  private Map<String, HdfsStorage> appIdToStorages = Maps.newConcurrentMap();
  private Map<String, HdfsStorage> pathToStorages = Maps.newConcurrentMap();

  HdfsStorageManager(ShuffleServerConf conf) {
    super(conf);
    hadoopConf = conf.getHadoopConf();
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalHdfsWriteDataSize.inc(event.getSize());
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    return getStorageByAppId(event.getAppId());
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return getStorageByAppId(event.getAppId());
  }

  @Override
  public void removeResources(String appId, Set<Integer> shuffleSet) {
    Storage storage = getStorageByAppId(appId);
    storage.removeHandlers(appId);
    appIdToStorages.remove(appId);
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest(StorageType.HDFS.name(), hadoopConf));
    deleteHandler.delete(new String[] {storage.getStoragePath()}, appId);
  }

  @Override
  public Checker getStorageChecker() {
    throw new RuntimeException("Not support storage checker");
  }

  @Override
  public void registerRemoteStorage(String appId, String remoteStorage) {
    if (!pathToStorages.containsKey(remoteStorage)) {
      pathToStorages.putIfAbsent(remoteStorage, new HdfsStorage(remoteStorage, hadoopConf));
    }
    appIdToStorages.putIfAbsent(appId, pathToStorages.get(remoteStorage));
  }

  private Storage getStorageByAppId(String appId) {
    if (!appIdToStorages.containsKey(appId)) {
      String msg = "Can't find HDFS storage for appId[" + appId + "]";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    return appIdToStorages.get(appId);
  }

  @VisibleForTesting
  public Map<String, HdfsStorage> getAppIdToStorages() {
    return appIdToStorages;
  }
}
