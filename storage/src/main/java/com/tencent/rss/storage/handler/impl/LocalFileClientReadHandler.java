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

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleIndexRequest;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.exception.RssException;

public class LocalFileClientReadHandler extends DataSkippableReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalFileClientReadHandler.class);
  private final int partitionNumPerRange;
  private final int partitionNum;
  private ShuffleServerClient shuffleServerClient;

  LocalFileClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      ShuffleServerClient shuffleServerClient) {
    super(appId, shuffleId, partitionId, readBufferSize, expectBlockIds, processBlockIds);
    this.shuffleServerClient = shuffleServerClient;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
  }

  @Override
  public ShuffleIndexResult readShuffleIndex() {
    ShuffleIndexResult shuffleIndexResult = null;
    RssGetShuffleIndexRequest request = new RssGetShuffleIndexRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);
    try {
      shuffleIndexResult = shuffleServerClient.getShuffleIndex(request).getShuffleIndexResult();
    } catch (Exception e) {
      throw new RssException("Failed to read shuffle index for appId[" + appId + "], shuffleId["
        + shuffleId + "], partitionId[" + partitionId + "] due to " + e.getMessage());
    }
    return shuffleIndexResult;
  }

  @Override
  public ShuffleDataResult readShuffleData(ShuffleDataSegment shuffleDataSegment) {
    ShuffleDataResult result = null;
    int expectedLength = shuffleDataSegment.getLength();
    if (expectedLength <= 0) {
      throw new RssException("Failed to read shuffle data for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "], "
          + "the length field in the index segment is " + expectedLength + " <= 0!");
    }
    RssGetShuffleDataRequest request = new RssGetShuffleDataRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum,
        shuffleDataSegment.getOffset(), expectedLength);
    try {
      RssGetShuffleDataResponse response = shuffleServerClient.getShuffleData(request);
      result = new ShuffleDataResult(response.getShuffleData(), shuffleDataSegment.getBufferSegments());
    } catch (Exception e) {
      throw new RssException("Failed to read shuffle data with "
          + shuffleServerClient.getClientInfo() + " due to " + e.getMessage());
    }
    if (result.getData().length != expectedLength) {
      throw new RssException("Wrong data length expect " + result.getData().length
          + " but actual is " + expectedLength);
    }
    return result;
  }
}
