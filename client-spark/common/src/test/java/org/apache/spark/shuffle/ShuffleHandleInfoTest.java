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

package org.apache.spark.shuffle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleHandleInfoTest {

  private ShuffleServerInfo createFakeServerInfo(String id) {
    return new ShuffleServerInfo(id, id, 1);
  }

  @Test
  public void testReassignment() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    ShuffleHandleInfo handleInfo =
        new ShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    assertFalse(handleInfo.isMarkedAsFaultyServer("a"));
    Set<Integer> partitions = Sets.newHashSet(1);
    handleInfo.updateReassignment(partitions, "a", Sets.newHashSet(createFakeServerInfo("d")));
    assertTrue(handleInfo.isMarkedAsFaultyServer("a"));
  }

  @Test
  public void testListAllPartitionAssignmentServers() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    ShuffleHandleInfo handleInfo =
        new ShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // case1
    Set<Integer> partitions = Sets.newHashSet(2);
    handleInfo.updateReassignment(partitions, "c", Sets.newHashSet(createFakeServerInfo("d")));

    Map<Integer, List<ShuffleServerInfo>> partitionAssignment =
        handleInfo.listPartitionAssignedServers();
    assertEquals(2, partitionAssignment.size());
    assertEquals(
        Arrays.asList(createFakeServerInfo("c"), createFakeServerInfo("d")),
        partitionAssignment.get(2));

    // case2: reassign multiple times for one partition, it will not append the same replacement
    // servers
    handleInfo.updateReassignment(partitions, "c", Sets.newHashSet(createFakeServerInfo("d")));
    partitionAssignment = handleInfo.listPartitionAssignedServers();
    assertEquals(
        Arrays.asList(createFakeServerInfo("c"), createFakeServerInfo("d")),
        partitionAssignment.get(2));

    // case3: reassign multiple times for one partition, it will append the non-existing replacement
    // servers
    handleInfo.updateReassignment(
        partitions, "c", Sets.newHashSet(createFakeServerInfo("d"), createFakeServerInfo("e")));
    partitionAssignment = handleInfo.listPartitionAssignedServers();
    assertEquals(
        Arrays.asList(
            createFakeServerInfo("c"), createFakeServerInfo("d"), createFakeServerInfo("e")),
        partitionAssignment.get(2));
  }

  @Test
  public void testCreatePartitionReplicaTracking() {
    ShuffleServerInfo a = createFakeServerInfo("a");
    ShuffleServerInfo b = createFakeServerInfo("b");
    ShuffleServerInfo c = createFakeServerInfo("c");

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(a, b));
    partitionToServers.put(2, Arrays.asList(c));

    ShuffleHandleInfo handleInfo =
        new ShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // not any replacements
    PartitionDataReplicaRequirementTracking tracking = handleInfo.createPartitionReplicaTracking();
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = tracking.getInventory();
    assertEquals(a, inventory.get(1).get(0).get(0));
    assertEquals(b, inventory.get(1).get(1).get(0));
    assertEquals(c, inventory.get(2).get(0).get(0));
  }
}
