/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.

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

package com.tencent.rss.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.PartitionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * PartitionBalanceAssignmentStrategy will consider allocating partitions from two aspects
 * (available memory and partitionAssignment).The strategy will sequentially process requests,
 * not concurrently. Because we don't want multiple requests to compete the same shuffle server.
 * We choose the shuffle server which give partitions the most available memory to allocate partitions;
 * For example:
 * There is three shuffle servers:
 * Initial Status:
 * S1 (2G, 0) S2 (5G, 0) S3(1G, 0)
 * First round, we request one partition, then
 * S1 (2G, 0) S2 (5G, 1) S3(1G, 0)
 * Second round, we request one partition, then
 * S1 (2, 0) S2 (5G, 2) s3(1G, 0), we request one partition, then
 * Third round, we request one partition, then
 * S1 (2, 1) S2 (5G, 2) s3(1G, 0)
 * ....
 **/

public class PartitionBalanceAssignmentStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBalanceAssignmentStrategy.class);

  private ClusterManager clusterManager;
  private Map<ServerNode, PartitionAssignmentInfo> serverToPartitions = Maps.newConcurrentMap();

  public PartitionBalanceAssignmentStrategy(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  @Override
  public PartitionRangeAssignment assign(
      int totalPartitionNum,
      int partitionNumPerRange,
      int replica,
      Set<String> requiredTags) {

    if (partitionNumPerRange != 1) {
      throw new RuntimeException("PartitionNumPerRange must be one");
    }

    SortedMap<PartitionRange, List<ServerNode>> assignments = new TreeMap<>();
    synchronized (this) {
        List<ServerNode> nodes = clusterManager.getServerList(requiredTags);
        Map<ServerNode, PartitionAssignmentInfo> newPartitionInfos = Maps.newConcurrentMap();
        for (ServerNode node : nodes) {
          PartitionAssignmentInfo partitionInfo;
          if (serverToPartitions.containsKey(node)) {
            partitionInfo = serverToPartitions.get(node);
            if (partitionInfo.getTimestamp() < node.getTimestamp()) {
              partitionInfo.resetPartitionNum();
              partitionInfo.setTimestamp(node.getTimestamp());
            }
          } else {
            partitionInfo = new PartitionAssignmentInfo();
          }
          newPartitionInfos.putIfAbsent(node, partitionInfo);
        }
        serverToPartitions = newPartitionInfos;
        int averagePartitions = totalPartitionNum * replica / clusterManager.getShuffleNodesMax();
        int assignPartitions = averagePartitions < 1 ? 1 : averagePartitions;
        nodes.sort(new Comparator<ServerNode>() {
          @Override
          public int compare(ServerNode o1, ServerNode o2) {
            PartitionAssignmentInfo partitionInfo1 = serverToPartitions.get(o1);
            PartitionAssignmentInfo partitionInfo2 = serverToPartitions.get(o2);
            double v1 = o1.getAvailableMemory() * 1.0 / (partitionInfo1.getPartitionNum() + assignPartitions);
            double v2 = o2.getAvailableMemory() * 1.0 / (partitionInfo2.getPartitionNum() + assignPartitions);
            return -Double.compare(v1, v2);
          }
        });

        if (nodes.isEmpty() || nodes.size() < replica) {
          throw new RuntimeException("There isn't enough shuffle servers");
        }

        int expectNum = clusterManager.getShuffleNodesMax();
        if (nodes.size() < clusterManager.getShuffleNodesMax()) {
          LOG.warn("Can't get expected servers [" + expectNum + "] and found only [" + nodes.size() + "]");
          expectNum = nodes.size();
        }

        List<ServerNode> candidatesNodes = nodes.subList(0, expectNum);
        int idx = 0;
        List<PartitionRange> ranges = CoordinatorUtils.generateRanges(totalPartitionNum, 1);
        for (PartitionRange range : ranges) {
          List<ServerNode> assignNodes = Lists.newArrayList();
          for (int rc = 0; rc < replica; rc++) {
            ServerNode node =  candidatesNodes.get(idx);
            idx = CoordinatorUtils.nextIdx(idx,  candidatesNodes.size());
            serverToPartitions.get(node).incrementPartitionNum();;
            assignNodes.add(node);
          }
          assignments.put(range, assignNodes);
        }
    }
    return new PartitionRangeAssignment(assignments);
  }

  @VisibleForTesting
  Map<ServerNode, PartitionAssignmentInfo> getServerToPartitions() {
    return serverToPartitions;
  }

  class PartitionAssignmentInfo {

    PartitionAssignmentInfo() {
      partitionNum = 0;
      timestamp = System.currentTimeMillis();
    }

    int partitionNum;
    long timestamp;

    public int getPartitionNum() {
      return partitionNum;
    }

    public void resetPartitionNum() {
      this.partitionNum = 0;
    }

    public void incrementPartitionNum() {
      partitionNum++;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
  }
}
