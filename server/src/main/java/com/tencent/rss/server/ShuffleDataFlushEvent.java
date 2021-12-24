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

import java.util.List;
import java.util.function.Supplier;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.server.buffer.ShuffleBuffer;

public class ShuffleDataFlushEvent {

  private long eventId;
  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private long size;
  private List<ShufflePartitionedBlock> shuffleBlocks;
  private Supplier<Boolean> valid = null;
  private ShuffleBuffer shuffleBuffer;

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long size,
      List<ShufflePartitionedBlock> shuffleBlocks,
      Supplier<Boolean> valid,
      ShuffleBuffer shuffleBuffer) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.size = size;
    this.shuffleBlocks = shuffleBlocks;
    this.valid = valid;
    this.shuffleBuffer = shuffleBuffer;
  }

  public List<ShufflePartitionedBlock> getShuffleBlocks() {
    return shuffleBlocks;
  }

  public long getEventId() {
    return eventId;
  }

  public long getSize() {
    return size;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public ShuffleBuffer getShuffleBuffer() {
    return shuffleBuffer;
  }

  public boolean isValid() {
    if (valid == null) {
      return true;
    }
    return valid.get();
  }

  @Override
  public String toString() {
    return "ShuffleDataFlushEvent: eventId=" + eventId
        + ", appId=" + appId
        + ", shuffleId=" + shuffleId
        + ", startPartition=" + startPartition
        + ", endPartition=" + endPartition;
  }
}
