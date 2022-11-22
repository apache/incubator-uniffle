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
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;

public class LocalFileQuorumClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileQuorumClientReadHandler.class);

  private List<LocalFileClientReadHandler> handlers = Lists.newLinkedList();

  private long readBlockNum = 0L;
  private long readLength = 0L;
  private long readUncompressLength = 0L;

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
      List<ShuffleServerClient> shuffleServerClients,
      ShuffleDataDistributionType distributionType,
      Roaring64NavigableMap expectTaskIds,
      int maxHandlerFailTimes) {
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
          client,
          distributionType,
          expectTaskIds,
          maxHandlerFailTimes
      ));
    }
  }

  /**
   * Only for test
   */
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
    this(
        appId, shuffleId, partitionId, indexReadLimit, partitionNumPerRange,
        partitionNum, readBufferSize, expectBlockIds, processBlockIds,
        shuffleServerClients, ShuffleDataDistributionType.NORMAL, Roaring64NavigableMap.bitmapOf(), 3
    );
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    if (finished()) {
      return null;
    }
    ShuffleDataResult result = null;
    for (LocalFileClientReadHandler handler : handlers) {
      if (handler.finished()) {
        continue;
      }
      try {
        result = handler.readShuffleData();
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read a replica due to ", e);
      }
    }
    return result;
  }

  @Override
  public void updateConsumedBlockInfo(BufferSegment bs) {
    if (bs == null) {
      return;
    }
    readBlockNum++;
    readLength += bs.getLength();
    readUncompressLength += bs.getUncompressLength();
  }

  @Override
  public void logConsumedBlockInfo() {
    LOG.info("Client read " + readBlockNum + " blocks,"
        + " bytes:" +  readLength + " uncompressed bytes:" + readUncompressLength);
  }

  @Override
  public boolean finished() {
    for (LocalFileClientReadHandler handler : handlers) {
      if (!handler.finished()) {
        return false;
      }
    }
    return true;
  }
}
