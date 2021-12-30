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

package com.tencent.rss.server.storage;

import java.util.List;

import com.google.common.collect.Lists;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.ShuffleServerMetrics;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.common.StorageWriteMetrics;


public abstract class SingleStorageManager implements StorageManager {

  private final long writeSlowThreshold;
  private final long eventSizeThresholdL1;
  private final long eventSizeThresholdL2;
  private final long eventSizeThresholdL3;

  public SingleStorageManager(ShuffleServerConf conf) {
    writeSlowThreshold = conf.getSizeAsBytes(ShuffleServerConf.SERVER_WRITE_SLOW_THRESHOLD);
    eventSizeThresholdL1 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L1);
    eventSizeThresholdL2 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L2);
    eventSizeThresholdL3 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L3);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    StorageWriteMetrics metrics = createStorageWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalWriteTime.inc(metrics.getWriteTime());
    ShuffleServerMetrics.counterWriteTotal.inc();
    if (metrics.getWriteTime() > writeSlowThreshold) {
      ShuffleServerMetrics.counterWriteSlow.inc();
    }
    ShuffleServerMetrics.counterTotalWriteDataSize.inc(metrics.getEventSize());
    ShuffleServerMetrics.counterTotalWriteBlockSize.inc(metrics.getWriteBlocks());
    if (metrics.getEventSize() < eventSizeThresholdL1) {
      ShuffleServerMetrics.counterEventSizeThresholdLevel1.inc();
    } else if (metrics.getEventSize() < eventSizeThresholdL2) {
      ShuffleServerMetrics.counterEventSizeThresholdLevel2.inc();
    } else if (metrics.getEventSize() < eventSizeThresholdL3) {
      ShuffleServerMetrics.counterEventSizeThresholdLevel3.inc();
    } else {
      ShuffleServerMetrics.counterEventSizeThresholdLevel4.inc();
    }
    Storage storage = selectStorage(event);
    storage.updateWriteMetrics(metrics);
  }

  public StorageWriteMetrics createStorageWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    long length = 0;
    long blockNum = 0;
    for (ShufflePartitionedBlock block : event.getShuffleBlocks()) {
      length += block.getLength();
      blockNum++;
    }
    List<Integer> partitions = Lists.newArrayList();
    for (int partition = event.getStartPartition(); partition <= event.getEndPartition(); partition++) {
      partitions.add(partition);
    }
    return new StorageWriteMetrics(
        event.getSize(),
        blockNum,
        writeTime,
        length,
        partitions,
        event.getAppId(),
        event.getShuffleId());
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void stop() {
    // do nothing
  }
}
