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

import com.google.common.collect.Lists;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleIndexRequest;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.RssUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LocalFileClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileClientReadHandler.class);
  private int partitionNumPerRange;
  private int partitionNum;
  private List<ShuffleServerClient> shuffleServerClients;
  private List<ShuffleDataSegment> shuffleDataSegments = Lists.newLinkedList();
  private int segmentIndex = 0;

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
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClients = shuffleServerClients;
  }

  public ShuffleIndexResult readShuffleIndex() {
    boolean readSuccessful = false;
    ShuffleIndexResult shuffleIndexResult = null;
    RssGetShuffleIndexRequest request = new RssGetShuffleIndexRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);

    for (ShuffleServerClient shuffleServerClient : shuffleServerClients) {
      try {
        shuffleIndexResult = shuffleServerClient.getShuffleIndex(request).getShuffleIndexResult();
        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read shuffle index with " + shuffleServerClient.getClientInfo(), e);
      }
    }

    if (!readSuccessful) {
      throw new RssException("Failed to read shuffle index for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "]");
    }

    return shuffleIndexResult;
  }

  public ShuffleDataResult readShuffleData(ShuffleDataSegment shuffleDataSegment) {
    boolean readSuccessful = false;
    ShuffleDataResult result = null;
    int expectedLength = shuffleDataSegment.getLength();

    if (expectedLength <= 0) {
      throw new RssException("Failed to read shuffle data for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "], "
          + "the length field in the index segment is " + expectedLength + " <= 0!");
    }

    RssGetShuffleDataRequest request = new RssGetShuffleDataRequest(
        appId,shuffleId, partitionId, partitionNumPerRange, partitionNum,
        shuffleDataSegment.getOffset(), expectedLength);

    for (ShuffleServerClient shuffleServerClient : shuffleServerClients) {
      try {
        RssGetShuffleDataResponse response = shuffleServerClient.getShuffleData(request);
        result = new ShuffleDataResult(response.getShuffleData(), shuffleDataSegment.getBufferSegments());

        if (result.getData().length != expectedLength) {
          throw new RssException("Wrong data length expect " + result.getData().length
              + " but actual is " + expectedLength);
        }

        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read shuffle data with " + shuffleServerClient.getClientInfo(), e);
      }
    }

    if (!readSuccessful) {
      throw new RssException("Failed to read shuffle data for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "]");
    }

    return result;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      shuffleDataSegments = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);
    }

    if (segmentIndex >= shuffleDataSegments.size()) {
      return null;
    }

    ShuffleDataResult sdr = readShuffleData(shuffleDataSegments.get(segmentIndex));
    segmentIndex++;
    return sdr;
  }
}
