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

package org.apache.uniffle.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.uniffle.common.ShuffleServerInfo;

/**
 * This class is to track the partition data replica requirements, which is used for {@link
 * org.apache.uniffle.client.impl.ShuffleWriteClientImpl} to check whether the reading blockIds from
 * multi/single server(s) meet the min replica.
 */
public class PartitionDataReplicaRequirementTracking {
  private int shuffleId;

  // partitionId -> replicaIndex -> shuffleServerInfo
  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory;

  private Map<Integer, Map<Integer, Integer>> succeedList = new HashMap<>();

  public PartitionDataReplicaRequirementTracking(
      int shuffleId, Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory) {
    this.shuffleId = shuffleId;
    this.inventory = inventory;
  }

  // for the DefaultShuffleHandleInfo
  public PartitionDataReplicaRequirementTracking(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers, int shuffleId) {
    this.shuffleId = shuffleId;
    this.inventory = toPartitionReplicaServers(partitionToServers);
  }

  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> toPartitionReplicaServers(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = new HashMap<>();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicas =
          inventory.computeIfAbsent(partitionId, x -> new HashMap<>());
      for (int i = 0; i < entry.getValue().size(); i++) {
        replicas.computeIfAbsent(i, x -> new ArrayList<>()).add(entry.getValue().get(i));
      }
    }
    return inventory;
  }

  public boolean isSatisfied(int partitionId, int minReplica) {
    // replica index -> successful count
    Map<Integer, Integer> succeedReplicas = succeedList.get(partitionId);
    if (succeedReplicas == null) {
      succeedReplicas = new HashMap<>();
    }

    Map<Integer, List<ShuffleServerInfo>> replicaList = inventory.get(partitionId);
    int replicaSuccessfulCnt = 0;
    for (Map.Entry<Integer, Integer> succeedReplica : succeedReplicas.entrySet()) {
      int replicaIndex = succeedReplica.getKey();
      int succeedCnt = succeedReplica.getValue();

      int expected = replicaList.get(replicaIndex).size();
      if (succeedCnt >= expected) {
        replicaSuccessfulCnt += 1;
      }
    }
    if (replicaSuccessfulCnt >= minReplica) {
      return true;
    }
    return false;
  }

  public void markPartitionOfServerSuccessful(int partitionId, ShuffleServerInfo server) {
    Map<Integer, Integer> partitionRequirements =
        succeedList.computeIfAbsent(partitionId, l -> new HashMap<>());

    Map<Integer, List<ShuffleServerInfo>> replicaServerChains = inventory.get(partitionId);
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : replicaServerChains.entrySet()) {
      int replicaIdx = entry.getKey();
      if (entry.getValue().contains(server)) {
        int old = partitionRequirements.computeIfAbsent(replicaIdx, x -> 0);
        partitionRequirements.put(replicaIdx, old + 1);
      }
    }
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public Map<Integer, Map<Integer, List<ShuffleServerInfo>>> getInventory() {
    return inventory;
  }

  @Override
  public String toString() {
    return "PartitionDataReplicaRequirementTracking{"
        + "shuffleId="
        + shuffleId
        + ", inventory="
        + inventory
        + ", succeedList="
        + succeedList
        + '}';
  }
}
