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

package org.apache.uniffle.storage.handler.impl;

import java.util.List;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;

public class MemoryQuorumClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryQuorumClientReadHandler.class);
  private long lastBlockId = Constants.INVALID_BLOCK_ID;
  private List<MemoryClientReadHandler> handlers = Lists.newLinkedList();
  private int currentHandlerIdx = 0;

  public MemoryQuorumClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      List<ShuffleServerClient> shuffleServerClients, Roaring64NavigableMap processBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    shuffleServerClients.forEach(client ->
        handlers.add(new MemoryClientReadHandler(
            appId, shuffleId, partitionId, readBufferSize, client, processBlockIds))
    );
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult result = null;
    while (currentHandlerIdx < handlers.size()) {
      try {
        result = RetryUtils.retry(() -> {
          MemoryClientReadHandler handler = handlers.get(currentHandlerIdx);
          ShuffleDataResult shuffleDataResult = handler.readShuffleData();
          return shuffleDataResult;
        }, 1000, 3);
      } catch (Throwable e) {
        LOG.warn("Failed to read a replica for appId[" + appId + "], shuffleId["
            + shuffleId + "], partitionId[" + partitionId + "] due to ", e);
      }

      if (result == null || result.isEmpty()) {
        currentHandlerIdx++;
        continue;
      }
      return result;
    }

    return result;
  }
}
