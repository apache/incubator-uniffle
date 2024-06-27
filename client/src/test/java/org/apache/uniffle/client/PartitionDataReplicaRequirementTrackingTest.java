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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionDataReplicaRequirementTrackingTest {

  @Test
  public void testSingleReplicaWithSingleShuffleServer() {
    // partitionId -> replicaIndex -> shuffleServerInfo
    ShuffleServerInfo s1 = new ShuffleServerInfo("s1", "1.1.1.1", 2);
    ShuffleServerInfo s2 = new ShuffleServerInfo("s2", "1.1.1.1", 3);

    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = new HashMap<>();

    Map<Integer, List<ShuffleServerInfo>> partition0 =
        inventory.computeIfAbsent(0, x -> new HashMap<>());
    partition0.put(0, Arrays.asList(s1));

    Map<Integer, List<ShuffleServerInfo>> partition1 =
        inventory.computeIfAbsent(1, x -> new HashMap<>());
    partition1.put(0, Arrays.asList(s2));

    PartitionDataReplicaRequirementTracking tracking =
        new PartitionDataReplicaRequirementTracking(1, inventory);
    assertFalse(tracking.isSatisfied(0, 1));
    assertFalse(tracking.isSatisfied(1, 1));

    tracking.markPartitionOfServerSuccessful(0, s1);
    assertTrue(tracking.isSatisfied(0, 1));
    assertFalse(tracking.isSatisfied(1, 1));

    tracking.markPartitionOfServerSuccessful(1, s2);
    assertTrue(tracking.isSatisfied(0, 1));
    assertTrue(tracking.isSatisfied(1, 1));
  }

  @Test
  public void testSingleReplicaWithMultiServers() {
    // partitionId -> replicaIndex -> shuffleServerInfo
    ShuffleServerInfo s1 = new ShuffleServerInfo("s1", "1.1.1.1", 2);
    ShuffleServerInfo s2 = new ShuffleServerInfo("s2", "1.1.1.1", 3);

    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = new HashMap<>();

    int partitionId = 0;
    Map<Integer, List<ShuffleServerInfo>> partition0 =
        inventory.computeIfAbsent(partitionId, x -> new HashMap<>());
    partition0.put(partitionId, Arrays.asList(s1));
    partition0.put(partitionId, Arrays.asList(s1, s2));

    PartitionDataReplicaRequirementTracking tracking =
        new PartitionDataReplicaRequirementTracking(1, inventory);
    assertFalse(tracking.isSatisfied(partitionId, 1));

    // mark the partition-0 with 1 server, it will fail.
    tracking.markPartitionOfServerSuccessful(partitionId, s1);
    assertFalse(tracking.isSatisfied(partitionId, 1));

    tracking.markPartitionOfServerSuccessful(partitionId, s1);
    assertTrue(tracking.isSatisfied(partitionId, 1));
  }

  @Test
  public void testMultipleReplicaWithSingleServer() {
    // partitionId -> replicaIndex -> shuffleServerInfo
    ShuffleServerInfo s1 = new ShuffleServerInfo("s1", "1.1.1.1", 2);
    ShuffleServerInfo s2 = new ShuffleServerInfo("s2", "1.1.1.1", 3);
    ShuffleServerInfo s3 = new ShuffleServerInfo("s3", "1.1.1.1", 3);

    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = new HashMap<>();
    int partitionId = 1;

    Map<Integer, List<ShuffleServerInfo>> partition1 =
        inventory.computeIfAbsent(partitionId, x -> new HashMap<>());

    // replicaIdx -> shuffle-servers
    partition1.put(0, Arrays.asList(s1));
    partition1.put(1, Arrays.asList(s2));
    partition1.put(2, Arrays.asList(s3));

    // partition1 has 3 replicas
    PartitionDataReplicaRequirementTracking tracking =
        new PartitionDataReplicaRequirementTracking(1, inventory);
    assertFalse(tracking.isSatisfied(partitionId, 1));

    tracking.markPartitionOfServerSuccessful(partitionId, s1);
    assertTrue(tracking.isSatisfied(partitionId, 1));
    assertFalse(tracking.isSatisfied(partitionId, 2));

    tracking.markPartitionOfServerSuccessful(partitionId, s2);
    assertTrue(tracking.isSatisfied(partitionId, 1));
    assertTrue(tracking.isSatisfied(partitionId, 2));
    assertFalse(tracking.isSatisfied(partitionId, 3));

    tracking.markPartitionOfServerSuccessful(partitionId, s3);
    assertTrue(tracking.isSatisfied(partitionId, 1));
    assertTrue(tracking.isSatisfied(partitionId, 2));
    assertTrue(tracking.isSatisfied(partitionId, 3));
  }

  @Test
  public void testMultipleReplicaWithMultiServers() {
    ShuffleServerInfo s1 = new ShuffleServerInfo("s1", "1.1.1.1", 2);
    ShuffleServerInfo s2 = new ShuffleServerInfo("s2", "1.1.1.1", 3);
    ShuffleServerInfo s3 = new ShuffleServerInfo("s3", "1.1.1.1", 3);
    ShuffleServerInfo s4 = new ShuffleServerInfo("s4", "1.1.1.1", 3);

    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = new HashMap<>();
    int partitionId = 0;

    Map<Integer, List<ShuffleServerInfo>> partition1 =
        inventory.computeIfAbsent(partitionId, x -> new HashMap<>());

    // replicaIdx -> shuffle-servers
    partition1.put(0, Arrays.asList(s1, s2));
    partition1.put(1, Arrays.asList(s3, s4));

    PartitionDataReplicaRequirementTracking tracking =
        new PartitionDataReplicaRequirementTracking(1, inventory);
    assertFalse(tracking.isSatisfied(partitionId, 1));

    tracking.markPartitionOfServerSuccessful(partitionId, s1);
    tracking.markPartitionOfServerSuccessful(partitionId, s3);
    assertFalse(tracking.isSatisfied(partitionId, 1));

    tracking.markPartitionOfServerSuccessful(partitionId, s2);
    assertTrue(tracking.isSatisfied(partitionId, 1));
    assertFalse(tracking.isSatisfied(partitionId, 2));

    tracking.markPartitionOfServerSuccessful(partitionId, s4);
    assertTrue(tracking.isSatisfied(partitionId, 1));
    assertTrue(tracking.isSatisfied(partitionId, 2));
  }
}
