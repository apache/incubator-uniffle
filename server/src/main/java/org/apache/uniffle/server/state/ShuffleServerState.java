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

package org.apache.uniffle.server.state;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.server.ShuffleTaskInfo;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;

public class ShuffleServerState {
  // ShuffleTaskManager
  private Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds;
  private Map<String, ShuffleTaskInfo> shuffleTaskInfos;
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds;

  // ShuffleBufferManager
  private long preAllocatedSize;
  private long inFlushSize;
  private long usedMemory;
  private long readDataMemory;
  private Map<String, Map<Integer, AtomicLong>> shuffleSizeMap;

  // ShuffleFlushManager
  private Map<String, Map<Integer, Roaring64NavigableMap>> committedBlockIds;

  private ShuffleServerState() {
    // ignore
  }

  public Map<String, Map<Integer, Roaring64NavigableMap[]>> getPartitionsToBlockIds() {
    return partitionsToBlockIds;
  }

  public Map<String, ShuffleTaskInfo> getShuffleTaskInfos() {
    return shuffleTaskInfos;
  }

  public Map<Long, PreAllocatedBufferInfo> getRequireBufferIds() {
    return requireBufferIds;
  }

  public long getPreAllocatedSize() {
    return preAllocatedSize;
  }

  public long getInFlushSize() {
    return inFlushSize;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public long getReadDataMemory() {
    return readDataMemory;
  }

  public Map<String, Map<Integer, AtomicLong>> getShuffleSizeMap() {
    return shuffleSizeMap;
  }

  public Map<String, Map<Integer, Roaring64NavigableMap>> getCommittedBlockIds() {
    return committedBlockIds;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ShuffleServerState state = new ShuffleServerState();

    public Builder partitionsToBlockIds(Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds) {
      this.state.partitionsToBlockIds = partitionsToBlockIds;
      return this;
    }

    public Builder shuffleTaskInfos(Map<String, ShuffleTaskInfo> shuffleTaskInfos) {
      this.state.shuffleTaskInfos = shuffleTaskInfos;
      return this;
    }

    public Builder requireBufferIds(Map<Long, PreAllocatedBufferInfo> requireBufferIds) {
      this.state.requireBufferIds = requireBufferIds;
      return this;
    }

    public Builder preAllocatedSize(long preAllocatedSize) {
      this.state.preAllocatedSize = preAllocatedSize;
      return this;
    }

    public Builder inFlushSize(long inFlushSize) {
      this.state.inFlushSize = inFlushSize;
      return this;
    }

    public Builder usedMemory(long usedMemory) {
      this.state.usedMemory = usedMemory;
      return this;
    }

    public Builder readDataMemory(long readDataMemory) {
      this.state.readDataMemory = readDataMemory;
      return this;
    }

    public Builder shuffleSizeMap(Map<String, Map<Integer, AtomicLong>> shuffleSizeMap) {
      this.state.shuffleSizeMap = shuffleSizeMap;
      return this;
    }

    public Builder committedBlockIds(
        Map<String, Map<Integer, Roaring64NavigableMap>> committedBlockIds) {
      this.state.committedBlockIds = committedBlockIds;
      return this;
    }

    public ShuffleServerState build() {
      return this.state;
    }
  }
}
