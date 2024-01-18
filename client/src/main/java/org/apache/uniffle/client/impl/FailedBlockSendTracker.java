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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;

public class FailedBlockSendTracker {

  private Map<Long, Set<TrackBlockStatus>> trackBlockStatusMap;

  public FailedBlockSendTracker() {
    this.trackBlockStatusMap = Maps.newConcurrentMap();
  }

  public void add(
      ShuffleBlockInfo shuffleBlockInfo,
      ShuffleServerInfo shuffleServerInfo,
      StatusCode statusCode) {
    trackBlockStatusMap
        .computeIfAbsent(shuffleBlockInfo.getBlockId(), s -> Sets.newConcurrentHashSet())
        .add(new TrackBlockStatus(shuffleBlockInfo, shuffleServerInfo, statusCode));
  }

  public void merge(FailedBlockSendTracker failedBlockSendTracker) {
    this.trackBlockStatusMap.putAll(failedBlockSendTracker.trackBlockStatusMap);
  }

  public void remove(long blockId) {
    trackBlockStatusMap.remove(blockId);
  }

  public void clear() {
    trackBlockStatusMap.clear();
  }

  public Set<Long> getFailedBlockIds() {
    return trackBlockStatusMap.keySet();
  }

  public Set<TrackBlockStatus> getFailedBlockStatus(Long blockId) {
    return trackBlockStatusMap.get(blockId);
  }

  public Set<ShuffleServerInfo> getFaultyShuffleServers() {
    return trackBlockStatusMap.values().stream()
        .flatMap(Collection::stream)
        .map(s -> s.getShuffleServerInfo())
        .collect(Collectors.toSet());
  }
}
