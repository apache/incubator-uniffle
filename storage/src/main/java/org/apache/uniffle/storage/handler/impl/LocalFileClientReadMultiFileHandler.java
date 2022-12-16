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

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;

public class LocalFileClientReadMultiFileHandler extends AbstractClientReadHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileClientReadMultiFileHandler.class);

  private int storageIndex = 0;
  private LocalFileClientReadHandler readHandler;
  private int currentHandlerReadCount = 0;

  private String appId;
  private int shuffleId;
  private int partitionId;
  private int indexReadLimit;
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private Roaring64NavigableMap expectBlockIds;
  private Roaring64NavigableMap processBlockIds;
  private ShuffleServerClient shuffleServerClient;
  private ShuffleDataDistributionType distributionType;
  private Roaring64NavigableMap expectTaskIds;

  public LocalFileClientReadMultiFileHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      ShuffleServerClient shuffleServerClient,
      ShuffleDataDistributionType distributionType,
      Roaring64NavigableMap expectTaskIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
    this.shuffleServerClient = shuffleServerClient;
    this.distributionType = distributionType;
    this.expectTaskIds = expectTaskIds;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    /**
     * step1: read shuffle index from storage-{i}
     * step2: read shuffle data  from storage-{i}
     * step3: if shuffle data is null, then read next index from storage-{i+1}.
     *        - if empty, return.
     *        - else, return step1
     */

    if (readHandler == null) {
      LOGGER.info("Initializing local file client read handler with storage index: {}", storageIndex);
      this.readHandler = initHandler(storageIndex);
    }

    ShuffleDataResult shuffleDataResult = readHandler.readShuffleData();

    if (shuffleDataResult == null && currentHandlerReadCount == 0) {
      return null;
    }

    if (shuffleDataResult == null) {
      readHandler.close();
      storageIndex += 1;
      this.readHandler = initHandler(storageIndex);
      shuffleDataResult = readHandler.readShuffleData();
      if (shuffleDataResult == null) {
        return null;
      }
      LOGGER.info("Switched next local file client read handler with storage index: {}", storageIndex);
    }

    currentHandlerReadCount++;
    return shuffleDataResult;
  }

  @Override
  public void close() {
    if (readHandler != null) {
      readHandler.close();
    }
  }

  private LocalFileClientReadHandler initHandler(int storageIndex) {
    LocalFileClientReadHandler handler = new LocalFileClientReadHandler(
        appId,
        shuffleId,
        partitionId,
        indexReadLimit,
        partitionNumPerRange,
        partitionNum,
        readBufferSize,
        expectBlockIds,
        processBlockIds,
        shuffleServerClient,
        distributionType,
        expectTaskIds,
        storageIndex
    );
    return handler;
  }
}
