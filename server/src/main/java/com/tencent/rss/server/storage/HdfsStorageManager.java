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

package com.tencent.rss.server.storage;

import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.RemoteStorageInfo;
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
    HdfsStorage storage = getStorageByAppId(appId);
    storage.removeHandlers(appId);
    appIdToStorages.remove(appId);
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest(StorageType.HDFS.name(), storage.getConf()));
    deleteHandler.delete(new String[] {storage.getStoragePath()}, appId);
  }

  @Override
  public Checker getStorageChecker() {
    throw new RuntimeException("Not support storage checker");
  }

  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    String remoteStorage = remoteStorageInfo.getPath();
    Map<String, String> remoteStorageConf = remoteStorageInfo.getConfItems();
    if (!pathToStorages.containsKey(remoteStorage)) {
      Configuration remoteStorageHadoopConf = new Configuration(hadoopConf);
      if (remoteStorageConf != null && remoteStorageConf.size() > 0) {
        for (Map.Entry<String, String> entry : remoteStorageConf.entrySet()) {
          remoteStorageHadoopConf.setStrings(entry.getKey(), entry.getValue());
        }
      }
      pathToStorages.putIfAbsent(remoteStorage, new HdfsStorage(remoteStorage, remoteStorageHadoopConf));
      // registerRemoteStorage may be called in different threads,
      // make sure metrics won't be created duplicated
      // there shouldn't have performance issue because
      // it will be called only few times according to the number of remote storage
      String storageHost = pathToStorages.get(remoteStorage).getStorageHost();
      ShuffleServerMetrics.addDynamicCounterForRemoteStorage(storageHost);
    }
    appIdToStorages.putIfAbsent(appId, pathToStorages.get(remoteStorage));
  }

  private HdfsStorage getStorageByAppId(String appId) {
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

  @VisibleForTesting
  public Map<String, HdfsStorage> getPathToStorages() {
    return pathToStorages;
  }
}
