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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;

public class MemoryQuorumClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryQuorumClientReadHandler.class);
  private long lastBlockId = Constants.INVALID_BLOCK_ID;
  private List<MemoryClientReadHandler> handlers = Lists.newLinkedList();
  private boolean readAfterFirstRound;

  public MemoryQuorumClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      List<ShuffleServerClient> shuffleServerClients) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    shuffleServerClients.forEach(client ->
        handlers.add(new MemoryClientReadHandler(
            appId, shuffleId, partitionId, readBufferSize, client))
    );
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    if (finished()) {
      return null;
    }
    boolean readSuccessful = false;
    ShuffleDataResult result = null;

    for (MemoryClientReadHandler handler: handlers) {
      if (handler.finished()) {
        continue;
      }
      try {
        result = handler.readShuffleData();
        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read a replica due to ", e);
      }
    }

    if (!readSuccessful && !readAfterFirstRound) {
      throw new RssException("Failed to read in memory shuffle data for appId[" + appId
          + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
    }

    return result;
  }

  @Override
  public void nextRound() {
    readAfterFirstRound = true;
  }

  @Override
  public boolean finished() {
    for (MemoryClientReadHandler handler : handlers) {
      if (!handler.finished()) {
        return false;
      }
    }
    return true;
  }
}
