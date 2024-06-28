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

package org.apache.uniffle.client.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;

public class FailedBlockSendTracker {

  /**
   * blockId -> list(trackingStatus)
   *
   * <p>This indicates the blockId latest sending status, and it will not store the resending
   * history. The list data structure is to describe the multiple servers for the multiple replica
   */
  private Map<Long, List<TrackingBlockStatus>> trackingBlockStatusMap;

  public FailedBlockSendTracker() {
    this.trackingBlockStatusMap = Maps.newConcurrentMap();
  }

  public void add(
      ShuffleBlockInfo shuffleBlockInfo,
      ShuffleServerInfo shuffleServerInfo,
      StatusCode statusCode) {
    trackingBlockStatusMap
        .computeIfAbsent(shuffleBlockInfo.getBlockId(), s -> Lists.newLinkedList())
        .add(new TrackingBlockStatus(shuffleBlockInfo, shuffleServerInfo, statusCode));
  }

  public void merge(FailedBlockSendTracker failedBlockSendTracker) {
    this.trackingBlockStatusMap.putAll(failedBlockSendTracker.trackingBlockStatusMap);
  }

  public void remove(long blockId) {
    trackingBlockStatusMap.remove(blockId);
  }

  public void clearAndReleaseBlockResources() {
    trackingBlockStatusMap.values().stream()
        .flatMap(x -> x.stream())
        .forEach(x -> x.getShuffleBlockInfo().executeCompletionCallback(true));
    trackingBlockStatusMap.clear();
  }

  public Set<Long> getFailedBlockIds() {
    return trackingBlockStatusMap.keySet();
  }

  public List<TrackingBlockStatus> getFailedBlockStatus(Long blockId) {
    return trackingBlockStatusMap.get(blockId);
  }

  public Set<ShuffleServerInfo> getFaultyShuffleServers() {
    return trackingBlockStatusMap.values().stream()
        .flatMap(Collection::stream)
        .map(s -> s.getShuffleServerInfo())
        .collect(Collectors.toSet());
  }
}
