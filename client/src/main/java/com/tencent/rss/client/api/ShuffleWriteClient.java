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

package com.tencent.rss.client.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;

public interface ShuffleWriteClient {

  SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList);

  void sendAppHeartbeat(String appId, long timeoutMs);

  void registerShuffle(ShuffleServerInfo shuffleServerInfo,
      String appId, int shuffleId, List<PartitionRange> partitionRanges);

  boolean sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps);

  void registerCoordinators(String coordinators);

  Map<String, String> fetchClientConf(int timeoutMs);

  String fetchRemoteStorage(String appId);

  void reportShuffleResult(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      String appId,
      int shuffleId,
      long taskAttemptId,
      Map<Integer, List<Long>> partitionToBlockIds,
      int bitmapNum);

  ShuffleAssignmentsInfo getShuffleAssignments(String appId, int shuffleId, int partitionNum,
      int partitionNumPerRange, Set<String> requiredTags);

  Roaring64NavigableMap getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, int partitionId);

  void close();
}
