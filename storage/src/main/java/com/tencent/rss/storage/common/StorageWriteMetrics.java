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

package com.tencent.rss.storage.common;

import java.util.List;

public class StorageWriteMetrics {

  private final String appId;
  private final int shuffleId;
  private final long eventSize;
  private final long writeBlocks;
  private final long writeTime;
  private final long dataSize;
  private final List<Integer> partitions;

  public StorageWriteMetrics(
      long eventSize,
      long writeBlocks,
      long writeTime,
      long dataSize,
      List<Integer> partitions,
      String appId,
      int shuffleId) {
    this.writeBlocks = writeBlocks;
    this.eventSize = eventSize;
    this.writeTime = writeTime;
    this.dataSize = dataSize;
    this.partitions = partitions;
    this.appId = appId;
    this.shuffleId = shuffleId;
  }

  public long getEventSize() {
    return eventSize;
  }

  public long getWriteBlocks() {
    return writeBlocks;
  }

  public long getWriteTime() {
    return writeTime;
  }

  public long getDataSize() {
    return dataSize;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
