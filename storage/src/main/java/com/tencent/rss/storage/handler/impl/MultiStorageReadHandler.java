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

package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.util.StorageType;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MultiStorageReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageReadHandler.class);

  private ClientReadHandler clientReadHandler;
  private CreateShuffleReadHandlerRequest fallbackRequest;
  private final Roaring64NavigableMap expectBlockIds;
  private final Roaring64NavigableMap processBlockIds;
  private int offsetIndex;

  public MultiStorageReadHandler(
      StorageType primary,
      StorageType secondary,
      CreateShuffleReadHandlerRequest request,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds) {
    request.setStorageType(primary.name());
    this.clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
    request.setStorageType(secondary.name());
    this.fallbackRequest = request;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
    this.offsetIndex = 0;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult result = null;
    try {
      result = clientReadHandler.readShuffleData();
    } catch (Exception e) {
      LOG.info("Failed to read data from primary", e);
    }
    if (result != null && !result.isEmpty()) {
      return result;
    } else {
      if (fallbackRequest != null && !checkBlocks()) {
        LOG.info("Fallback to read data from secondary {}", fallbackRequest.getStorageType());
        clientReadHandler.close();
        clientReadHandler = createShuffleRemoteStorageReadHandler(fallbackRequest);
        fallbackRequest = null;
        result = clientReadHandler.readShuffleData();
        if (result != null && !result.isEmpty()) {
          return result;
        }
      }
    }
    return null;
  }

  @Override
  public void close() {
    clientReadHandler.close();
  }

  private boolean checkBlocks() {
    Roaring64NavigableMap cloneBitmap = cloneBitmap(expectBlockIds);
    cloneBitmap.and(processBlockIds);
    return cloneBitmap.equals(expectBlockIds);
  }

  private Roaring64NavigableMap cloneBitmap(Roaring64NavigableMap bitmap) {
    Roaring64NavigableMap cloneBitmap;
    try {
      cloneBitmap = RssUtils.deserializeBitMap(RssUtils.serializeBitMap(bitmap));
    } catch (IOException ioe) {
      throw new RuntimeException("clone bitmap exception", ioe);
    }
    return cloneBitmap;
  }

  private ClientReadHandler createShuffleRemoteStorageReadHandler(CreateShuffleReadHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new MultiStorageHdfsClientReadHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getPartitionId(),
          request.getIndexReadLimit(),
          request.getPartitionNumPerRange(),
          request.getPartitionNum(),
          request.getReadBufferSize(),
          request.getStorageBasePath(),
          request.getHadoopConf());
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client remote storage read handler:" + request.getStorageType());
    }
  }
}
