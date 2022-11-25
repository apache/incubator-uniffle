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

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;

public class MultiReplicaClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MultiReplicaClientReadHandler.class);

  private final List<ClientReadHandler> handlers;

  private long readBlockNum = 0L;
  private long readLength = 0L;
  private long readUncompressLength = 0L;
  private final Roaring64NavigableMap blockIdBitmap;
  private final Roaring64NavigableMap processedBlockIds;

  private int readHandlerIndex;

  public MultiReplicaClientReadHandler(
      List<ClientReadHandler> handlers,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap processedBlockIds) {
    this.handlers = handlers;
    this.blockIdBitmap = blockIdBitmap;
    this.processedBlockIds = processedBlockIds;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ClientReadHandler handler;
    ShuffleDataResult result = null;
    do {
      if (readHandlerIndex >= handlers.size()) {
        return result;
      }
      handler = handlers.get(readHandlerIndex);
      try {
        result = handler.readShuffleData();
      } catch (Exception e) {
        LOG.warn("Failed to read a replica due to ", e);
      }
      if (result != null && !result.isEmpty()) {
        return result;
      } else {
        readHandlerIndex++;
        try {
          RssUtils.checkProcessedBlockIds(blockIdBitmap, processedBlockIds);
          return result;
        } catch (RssException e) {
          LOG.warn(e.getMessage());
        }
      }
    } while (true);
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
}
