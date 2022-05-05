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

package com.tencent.rss.storage.common;

import org.apache.hadoop.conf.Configuration;

import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;

public class HdfsStorage extends AbstractStorage {

  private final String storagePath;
  private final Configuration conf;

  public HdfsStorage(String path, Configuration conf) {
    this.storagePath = path;
    this.conf = conf;
  }

  @Override
  public String getStoragePath() {
    return storagePath;
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public boolean lockShuffleShared(String shuffleKey) {
    return true;
  }

  @Override
  public boolean unlockShuffleShared(String shuffleKey) {
    return true;
  }

  @Override
  public boolean lockShuffleExcluded(String shuffleKey) {
    return true;
  }

  @Override
  public boolean unlockShuffleExcluded(String shuffleKey) {
    return true;
  }

  @Override
  public void updateReadMetrics(StorageReadMetrics metrics) {
    // do nothing
  }

  @Override
  public void updateWriteMetrics(StorageWriteMetrics metrics) {
    // do nothing
  }

  @Override
  ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request) {
    try {
      return new HdfsShuffleWriteHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getStartPartition(),
          request.getEndPartition(),
          storagePath,
          request.getFileNamePrefix(),
          request.getConf()
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request) {
    throw new RuntimeException("Hdfs storage don't support to read from sever");
  }

  @Override
  public void createMetadataIfNotExist(String shuffleKey) {
    // do nothing
  }
}
