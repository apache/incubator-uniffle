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

package com.tencent.rss.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Sets;
import com.tencent.rss.common.PartitionRange;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BasicAssignmentStrategyTest {

  Set<String> tags = Sets.newHashSet("test");
  private SimpleClusterManager clusterManager;
  private BasicAssignmentStrategy strategy;
  private int shuffleNodesMax = 7;

  @BeforeEach
  public void setUp() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    clusterManager = new SimpleClusterManager(ssc, new Configuration());
    strategy = new BasicAssignmentStrategy(clusterManager);
  }

  @AfterEach
  public void tearDown() throws IOException {
    clusterManager.clear();
    clusterManager.close();
  }

  @Test
  public void testAssign() {
    for (int i = 0; i < 20; ++i) {
      clusterManager.add(new ServerNode(String.valueOf(i), "", 0, 0, 0,
          20 - i, 0, tags, true));
    }

    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    assertEquals(10, assignments.size());

    for (int i = 0; i < 100; i += 10) {
      assignments.containsKey(new PartitionRange(i, i + 10));
    }

    int i = 0;
    Iterator<List<ServerNode>> ite = assignments.values().iterator();
    while (ite.hasNext()) {
      List<ServerNode> cur = ite.next();
      assertEquals(2, cur.size());
      assertEquals(String.valueOf(i % shuffleNodesMax), cur.get(0).getId());
      i++;
      assertEquals(String.valueOf(i % shuffleNodesMax), cur.get(1).getId());
      i++;
    }
  }

  @Test
  public void testRandomAssign() {
    for (int i = 0; i < 20; ++i) {
      clusterManager.add(new ServerNode(String.valueOf(i), "", 0, 0, 0,
          0, 0, tags, true));
    }
    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    Set<ServerNode> serverNodes1 = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes1.addAll(assignment.getValue());
    }

    pra = strategy.assign(100, 10, 2, tags);
    assignments = pra.getAssignments();
    Set<ServerNode> serverNodes2 = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes2.addAll(assignment.getValue());
    }

    // test for the random node pick, there is a little possibility failed
    assertFalse(serverNodes1.containsAll(serverNodes2));
  }

  @Test
  public void testAssignWithDifferentNodeNum() {
    ServerNode sn1 = new ServerNode("sn1", "", 0, 0, 0,
        20, 0, tags, true);
    ServerNode sn2 = new ServerNode("sn2", "", 0, 0, 0,
        10, 0, tags, true);
    ServerNode sn3 = new ServerNode("sn3", "", 0, 0, 0,
        0, 0, tags, true);

    clusterManager.add(sn1);
    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
    // nodeNum < replica
    assertNull(pra.getAssignments());

    // nodeNum = replica
    clusterManager.add(sn2);
    pra = strategy.assign(100, 10, 2, tags);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    Set<ServerNode> serverNodes = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes.addAll(assignment.getValue());
    }
    assertEquals(2, serverNodes.size());
    assertTrue(serverNodes.contains(sn1));
    assertTrue(serverNodes.contains(sn2));

    // nodeNum > replica & nodeNum < shuffleNodesMax
    clusterManager.add(sn3);
    pra = strategy.assign(100, 10, 2, tags);
    assignments = pra.getAssignments();
    serverNodes = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes.addAll(assignment.getValue());
    }
    assertEquals(3, serverNodes.size());
    assertTrue(serverNodes.contains(sn1));
    assertTrue(serverNodes.contains(sn2));
    assertTrue(serverNodes.contains(sn3));
  }
}
