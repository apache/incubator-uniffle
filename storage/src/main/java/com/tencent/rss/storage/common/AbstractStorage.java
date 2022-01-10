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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;

public abstract class AbstractStorage implements Storage {

  private Map<String, Map<String, ShuffleWriteHandler>> handlers = Maps.newConcurrentMap();
  private Map<String, Map<String, CreateShuffleWriteHandlerRequest>> requests = Maps.newConcurrentMap();

  abstract ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request);

  @Override
  public ShuffleWriteHandler getOrCreateWriteHandler(CreateShuffleWriteHandlerRequest request) {

    handlers.computeIfAbsent(request.getAppId(), key -> Maps.newConcurrentMap());
    requests.computeIfAbsent(request.getAppId(), key -> Maps.newConcurrentMap());
    Map<String, ShuffleWriteHandler> map = handlers.get(request.getAppId());
    String partitionKey = RssUtils.generatePartitionKey(
        request.getAppId(),
        request.getShuffleId(),
        request.getStartPartition()
    );
    map.computeIfAbsent(partitionKey, key -> newWriteHandler(request));
    Map<String, CreateShuffleWriteHandlerRequest> requestMap = requests.get(request.getAppId());
    requestMap.putIfAbsent(partitionKey, request);
    return map.get(partitionKey);
  }

  @Override
  public CreateShuffleWriteHandlerRequest getCreateWriterHandlerRequest(
      String appId,
      int shuffleId,
      int partition) {
    Map<String, CreateShuffleWriteHandlerRequest> requestMap = requests.get(appId);
    if (requestMap != null) {
      return requestMap.get(RssUtils.generatePartitionKey(appId, shuffleId, partition));
    }
    return null;
  }

  @Override
  public void removeHandlers(String appId) {
    handlers.remove(appId);
    requests.remove(appId);
  }

  @VisibleForTesting
  public int getHandlerSize() {
    return handlers.size();
  }
}
