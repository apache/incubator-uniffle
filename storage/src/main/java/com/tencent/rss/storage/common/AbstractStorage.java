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

package com.tencent.rss.storage.common;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

public abstract class AbstractStorage implements Storage {

  private Map<String, Map<String, ShuffleWriteHandler>> writerHandlers = Maps.newConcurrentMap();
  private Map<String, Map<String, CreateShuffleWriteHandlerRequest>> requests = Maps.newConcurrentMap();
  private Map<String, Map<String, ServerReadHandler>> readerHandlers = Maps.newConcurrentMap();

  abstract ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request);

  @Override
  public ShuffleWriteHandler getOrCreateWriteHandler(CreateShuffleWriteHandlerRequest request) {
    writerHandlers.computeIfAbsent(request.getAppId(), key -> Maps.newConcurrentMap());
    requests.computeIfAbsent(request.getAppId(), key -> Maps.newConcurrentMap());
    Map<String, ShuffleWriteHandler> map = writerHandlers.get(request.getAppId());
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
  public ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request) {
    readerHandlers.computeIfAbsent(request.getAppId(), key -> Maps.newConcurrentMap());
    Map<String, ServerReadHandler> map = readerHandlers.get(request.getAppId());
    int[] range = ShuffleStorageUtils.getPartitionRange(
        request.getPartitionId(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum());
    String partitionKey = RssUtils.generatePartitionKey(
        request.getAppId(),
        request.getShuffleId(),
        range[0]
    );
    map.computeIfAbsent(partitionKey, key -> newReadHandler(request));
    return map.get(partitionKey);
  }

  protected abstract ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request);

  public boolean containsWriteHandler(String appId, int shuffleId, int partition) {
    String partitionKey = RssUtils.generatePartitionKey(appId, shuffleId, partition);
    return writerHandlers.containsKey(partitionKey);
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
    writerHandlers.remove(appId);
    readerHandlers.remove(appId);
    requests.remove(appId);
  }

  @VisibleForTesting
  public int getHandlerSize() {
    return writerHandlers.size();
  }
}
