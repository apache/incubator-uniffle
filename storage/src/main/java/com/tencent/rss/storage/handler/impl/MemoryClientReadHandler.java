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

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssGetInMemoryShuffleDataRequest;
import com.tencent.rss.client.response.RssGetInMemoryShuffleDataResponse;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MemoryClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryClientReadHandler.class);
  private long lastBlockId = Constants.INVALID_BLOCK_ID;
  private List<ShuffleServerClient> shuffleServerClients;

  public MemoryClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      List<ShuffleServerClient> shuffleServerClients) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClients = shuffleServerClients;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    boolean readSuccessful = false;
    ShuffleDataResult result = null;

    RssGetInMemoryShuffleDataRequest request = new RssGetInMemoryShuffleDataRequest(
        appId,shuffleId, partitionId, lastBlockId, readBufferSize);

    for (ShuffleServerClient shuffleServerClient : shuffleServerClients) {
      try {
        RssGetInMemoryShuffleDataResponse response =
            shuffleServerClient.getInMemoryShuffleData(request);
        result = new ShuffleDataResult(response.getData(), response.getBufferSegments());

        readSuccessful = true;
        break;
      } catch (Exception e) {
        // todo: fault tolerance solution should be added
        LOG.warn("Failed to read in memory shuffle data with {}",
            shuffleServerClient.getClientInfo(), e);
      }
    }

    if (!readSuccessful) {
      throw new RssException("Failed to read in memory shuffle data for appId[" + appId
          + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
    }

    // update lastBlockId for next rpc call
    if (!result.isEmpty()) {
      List<BufferSegment> bufferSegments = result.getBufferSegments();
      lastBlockId = bufferSegments.get(bufferSegments.size() - 1).getBlockId();
    }

    return result;
  }
}
