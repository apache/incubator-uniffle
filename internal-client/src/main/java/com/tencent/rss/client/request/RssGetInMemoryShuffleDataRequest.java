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

package com.tencent.rss.client.request;

public class RssGetInMemoryShuffleDataRequest {
  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final long lastBlockId;
  private final int readBufferSize;

  public RssGetInMemoryShuffleDataRequest(
      String appId, int shuffleId, int partitionId, long lastBlockId, int readBufferSize) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.lastBlockId = lastBlockId;
    this.readBufferSize = readBufferSize;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getLastBlockId() {
    return lastBlockId;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }
}
