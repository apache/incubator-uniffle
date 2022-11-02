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

package org.apache.uniffle.client.request;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RssGetInMemoryShuffleDataRequest {
  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final long lastBlockId;
  private final int readBufferSize;
  private Roaring64NavigableMap processedBlockIds;
  private Roaring64NavigableMap expectBlockIds;

  public RssGetInMemoryShuffleDataRequest(
      String appId, int shuffleId, int partitionId, long lastBlockId, int readBufferSize,
      Roaring64NavigableMap processedBlockIds, Roaring64NavigableMap expectBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.lastBlockId = lastBlockId;
    this.readBufferSize = readBufferSize;
    this.processedBlockIds = processedBlockIds;
    this.expectBlockIds = expectBlockIds;
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

  public Roaring64NavigableMap getProcessedBlockIds() {
    return processedBlockIds;
  }

  public Roaring64NavigableMap getExpectBlockIds() {
    return expectBlockIds;
  }
}
