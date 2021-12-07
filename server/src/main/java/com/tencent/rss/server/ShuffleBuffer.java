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

package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public class ShuffleBuffer {

  private final long capacity;

  private long size;
  private List<ShufflePartitionedBlock> blocks;

  public ShuffleBuffer(long capacity) {
    this.capacity = capacity;
    this.size = 0;
    this.blocks = new LinkedList<>();
  }

  public int append(ShufflePartitionedData data) {
    int mSize = 0;

    synchronized (this) {
      for (ShufflePartitionedBlock block : data.getBlockList()) {
        blocks.add(block);
        mSize += block.getSize();
        size += mSize;
      }
    }

    return mSize;
  }

  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId, int shuffleId, int startPartition, int endPartition, Supplier<Boolean> isValid) {
    if (blocks.isEmpty()) {
      return null;
    }
    // buffer will be cleared, and new list must be created for async flush
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(blocks);
    ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement(),
        appId,
        shuffleId,
        startPartition,
        endPartition,
        size,
        spBlocks,
        isValid);
    blocks.clear();
    size = 0;
    return event;
  }

  public List<ShufflePartitionedBlock> getBlocks() {
    return blocks;
  }

  public long getSize() {
    return size;
  }

  public boolean isFull() {
    return size > capacity;
  }

}
