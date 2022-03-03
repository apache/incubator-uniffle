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

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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

  private final String storageBasePath;
  private final HdfsStorage storage;
  private final Configuration hadoopConf;

  HdfsStorageManager(ShuffleServerConf conf) {
    super(conf);
    if (StringUtils.isEmpty(conf.getString(ShuffleServerConf.HDFS_BASE_PATH))) {
      storageBasePath = conf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH);
    } else {
      storageBasePath = conf.getString(ShuffleServerConf.HDFS_BASE_PATH);
    }
    if (StringUtils.isEmpty(storageBasePath)) {
      throw new IllegalArgumentException("hdfs base path is empty");
    }
    hadoopConf = conf.getHadoopConf();
    storage = new HdfsStorage(storageBasePath, hadoopConf);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalHdfsWriteDataSize.inc(event.getSize());
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    return storage;
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return storage;
  }

  @Override
  public void removeResources(String appId, Set<Integer> shuffleSet) {
   storage.removeHandlers(appId);
   ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
       .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest(StorageType.HDFS.name(), hadoopConf));
   deleteHandler.delete(new String[] {storageBasePath}, appId);
  }

  @Override
  public Checker getStorageChecker() {
    throw new RuntimeException("Not support storage checker");
  }
}
