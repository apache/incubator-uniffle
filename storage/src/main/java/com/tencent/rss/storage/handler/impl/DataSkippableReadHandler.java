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

import java.util.List;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;

public abstract class DataSkippableReadHandler extends AbstractClientReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkippableReadHandler.class);

  protected List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  protected int segmentIndex = 0;

  protected Roaring64NavigableMap expectBlockIds;
  protected Roaring64NavigableMap processBlockIds;

  public DataSkippableReadHandler(
    String appId,
    int shuffleId,
    int partitionId,
    int readBufferSize,
    Roaring64NavigableMap expectBlockIds,
    Roaring64NavigableMap processBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
  }

  protected abstract ShuffleIndexResult readShuffleIndex();

  protected abstract ShuffleDataResult readShuffleData(ShuffleDataSegment segment);

  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      shuffleDataSegments = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);
    }

    // We should skip unexpected and processed segments when handler is read
    ShuffleDataResult result = null;
    while (segmentIndex < shuffleDataSegments.size()) {
      ShuffleDataSegment segment = shuffleDataSegments.get(segmentIndex);
      Roaring64NavigableMap blocksOfSegment = Roaring64NavigableMap.bitmapOf();
      segment.getBufferSegments().forEach(block -> blocksOfSegment.addLong(block.getBlockId()));
      // skip unexpected blockIds
      blocksOfSegment.and(expectBlockIds);
      if (!blocksOfSegment.isEmpty()) {
        // skip processed blockIds
        blocksOfSegment.or(processBlockIds);
        blocksOfSegment.xor(processBlockIds);
        if (!blocksOfSegment.isEmpty()) {
          result = readShuffleData(segment);
          segmentIndex++;
          break;
        }
      }
      segmentIndex++;
    }
    return result;
  }
}
