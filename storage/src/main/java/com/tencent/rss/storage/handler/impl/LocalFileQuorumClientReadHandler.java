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

import java.util.List;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.exception.RssException;

public class LocalFileQuorumClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileQuorumClientReadHandler.class);

  private List<LocalFileClientReadHandler> handlers = Lists.newLinkedList();

  public LocalFileQuorumClientReadHandler(
    String appId,
    int shuffleId,
    int partitionId,
    int indexReadLimit,
    int partitionNumPerRange,
    int partitionNum,
    int readBufferSize,
    Roaring64NavigableMap expectBlockIds,
    Roaring64NavigableMap processBlockIds,
    List<ShuffleServerClient> shuffleServerClients) {
      this.appId = appId;
      this.shuffleId = shuffleId;
      this.partitionId = partitionId;
      this.readBufferSize = readBufferSize;
      for (ShuffleServerClient client: shuffleServerClients) {
        handlers.add(new LocalFileClientReadHandler(
          appId,
          shuffleId,
          partitionId,
          indexReadLimit,
          partitionNumPerRange,
          partitionNum,
          readBufferSize,
          expectBlockIds,
          processBlockIds,
          client
        ));
      }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    boolean readSuccessful = false;
    ShuffleDataResult result = null;
    for (LocalFileClientReadHandler handler : handlers) {
      try {
        result = handler.readShuffleData();
        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read a replica due to ", e);
      }
    }
    if (!readSuccessful) {
      throw new RssException("Failed to read all replicas for appId[" + appId + "], shuffleId["
        + shuffleId + "], partitionId[" + partitionId + "]");
    }
    return result;
  }
}
