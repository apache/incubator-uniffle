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
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.ShuffleDataResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileClientReadHandler extends AbstractFileClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileClientReadHandler.class);
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private List<ShuffleServerClient> shuffleServerClients;

  public LocalFileClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      List<ShuffleServerClient> shuffleServerClients) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClients = shuffleServerClients;
  }

  @Override
  public ShuffleDataResult readShuffleData(int segmentIndex) {
    boolean readSuccessful = false;
    ShuffleDataResult result = null;
    RssGetShuffleDataRequest request = new RssGetShuffleDataRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum, readBufferSize, segmentIndex);
    for (ShuffleServerClient shuffleServerClient : shuffleServerClients) {
      try {
        RssGetShuffleDataResponse response = shuffleServerClient.getShuffleData(request);
        result = response.getShuffleDataResult();
        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read shuffle data with " + shuffleServerClient.getClientInfo(), e);
      }
    }
    if (!readSuccessful) {
      throw new RuntimeException("Failed to read shuffle data for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "]");
    }
    return result;
  }

  @Override
  public void close() {
  }
}
