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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.ShuffleUploader;
import com.tencent.rss.storage.common.LocalStorage;
import com.tencent.rss.storage.common.Storage;

public class MultiStorageManager implements StorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageManager.class);

  private final StorageManager warmStorageManager;
  private final StorageManager coldStorageManager;
  private final List<ShuffleUploader> uploaders  = Lists.newArrayList();
  private final boolean uploadShuffleEnable;
  private final long flushColdStorageThresholdSize;

  MultiStorageManager(ShuffleServerConf conf, String shuffleServerId) {
    warmStorageManager = new LocalStorageManager(conf);
    coldStorageManager = new HdfsStorageManager(conf);
    uploadShuffleEnable = conf.get(ShuffleServerConf.UPLOADER_ENABLE);
    flushColdStorageThresholdSize = conf.getSizeAsBytes(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE);
    if (uploadShuffleEnable) {
      if (!(warmStorageManager instanceof LocalStorageManager)) {
        throw new IllegalArgumentException("Only LOCALFILE type support upload shuffle");
      }
      LocalStorageManager localStorageManager = (LocalStorageManager) warmStorageManager;
      for (LocalStorage storage :localStorageManager.getStorages()) {
        uploaders.add(new ShuffleUploader.Builder()
            .configuration(conf)
            .serverId(shuffleServerId)
            .localStorage(storage)
            .build());
      }
    }
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    return selectStorageManager(event).selectStorage(event);
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return warmStorageManager.selectStorage(event);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    selectStorageManager(event).updateWriteMetrics(event, writeTime);
  }

  private StorageManager selectStorageManager(ShuffleDataFlushEvent event) {
    if (event.getSize() > flushColdStorageThresholdSize) {
      return coldStorageManager;
    } else {
      return warmStorageManager;
    }
  }

  public void start() {
    if (uploadShuffleEnable) {
      for (ShuffleUploader uploader : uploaders) {
        uploader.start();
      }
    }
  }

  public void stop() {
    if (uploadShuffleEnable) {
      for (ShuffleUploader uploader : uploaders) {
        uploader.stop();
      }
    }
  }

  @Override
  public void removeResources(String appId, Set<Integer> shuffleSet) {
    LOG.info("Start to remove resource of appId: {}, shuffles: {}", appId, shuffleSet.toString());
    warmStorageManager.removeResources(appId, shuffleSet);
    coldStorageManager.removeResources(appId, shuffleSet);
  }
}
