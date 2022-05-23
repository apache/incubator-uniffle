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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class PartitionBalanceAssignmentStrategyTest {

  private SimpleClusterManager clusterManager;
  private PartitionBalanceAssignmentStrategy strategy;
  private int shuffleNodesMax = 5;
  private Set<String> tags = Sets.newHashSet("test");

  @BeforeEach
  public void setUp() {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    clusterManager = new SimpleClusterManager(ssc);
    strategy = new PartitionBalanceAssignmentStrategy(clusterManager);
  }

  @Test
  public void testAssign() {
    List<Long> list = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      list.add(10L);
    }
    updateServerResource(list);
    boolean isThrown = false;
    try {
      strategy.assign(100, 2, 1, tags);
    } catch (Exception e) {
      isThrown = true;
    }
    assertTrue(isThrown);
    try {
      strategy.assign(0, 1, 1, tags);
    } catch (Exception e) {
      fail();
    }
    isThrown = false;
    try {
      strategy.assign(10, 1, 1, Sets.newHashSet("fake"));
    } catch (Exception e) {
      isThrown = true;
    }
    assertTrue(isThrown);
    strategy.assign(100, 1, 1, tags);
    List<Long> expect = Lists.newArrayList(20L, 20L, 20L, 20L, 20L, 0L, 0L, 0L, 0L,
        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);
    strategy.assign(75, 1, 1, tags);
    expect = Lists.newArrayList(20L, 20L, 20L, 20L, 20L, 15L, 15L, 15L, 15L,
        15L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);
    strategy.assign(100, 1, 1, tags);
    expect = Lists.newArrayList(20L, 20L, 20L, 20L, 20L, 15L, 15L, 15L, 15L,
        15L, 20L, 20L, 20L, 20L, 20L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);

    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    list = Lists.newArrayList(7L, 18L, 7L, 3L, 19L, 15L, 11L, 10L, 16L, 11L,
        14L, 17L, 15L, 17L, 8L, 1L, 3L, 3L, 6L, 12L);
    updateServerResource(list);
    strategy.assign(100, 1, 1, tags);
    expect = Lists.newArrayList(0L, 20L, 0L, 0L, 20L, 0L, 0L, 0L, 20L, 0L,
        0L, 20L, 0L, 20L, 0L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);
    strategy.assign(50, 1, 1, tags);
    expect = Lists.newArrayList(0L, 20L, 0L, 0L, 20L, 10L, 10L, 0L, 20L, 0L,
        10L, 20L, 10L, 20L, 0L, 0L, 0L, 0L, 0L, 10L);
    valid(expect);

    strategy.assign(75, 1, 1, tags);
    expect = Lists.newArrayList(0L, 20L, 0L, 0L, 20L, 25L, 10L, 15L, 20L, 15L,
        25L, 20L, 25L, 20L, 0L, 0L, 0L, 0L, 0L, 10L);
    valid(expect);

    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    list = Lists.newArrayList(7L, 18L, 7L, 3L, 19L, 15L, 11L, 10L, 16L, 11L,
        14L, 17L, 15L, 17L, 8L, 1L, 3L, 3L, 6L, 12L);
    updateServerResource(list);
    strategy.assign(50, 1, 2, tags);
    expect = Lists.newArrayList(0L, 20L, 0L, 0L, 20L, 0L, 0L, 0L, 20L, 0L,
        0L, 20L, 0L, 20L, 0L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);
    strategy.assign(75, 1, 2, tags);
    expect = Lists.newArrayList(0L, 20L, 0L, 0L, 50L, 30L, 0L, 0L, 20L, 0L,
        30L, 20L, 30L, 20L, 0L, 0L, 0L, 0L, 0L, 30L);
    valid(expect);
    strategy.assign(33, 1, 2, tags);
    expect = Lists.newArrayList(0L, 33L, 0L, 0L, 50L, 30L, 14L, 13L, 20L, 13L,
        30L, 20L, 30L, 20L, 13L, 0L, 0L, 0L, 0L, 30L);
    valid(expect);

    list = Lists.newArrayList();
    for (int i = 0; i< 20; i++) {
      if (i % 2 == 0) {
        list.add(10L);
      } else {
        list.add(20L);
      }
    }

    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    updateServerResource(list);
    strategy.assign(33, 1, 1, tags);
    expect = Lists.newArrayList(0L, 7L, 0L, 7L, 0L, 7L, 0L, 6L, 0L, 6L, 0L, 0L,
        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    valid(expect);
    strategy.assign(41, 1, 2, tags);
    expect = Lists.newArrayList(0L, 7L, 0L, 7L, 0L, 7L, 0L, 6L, 0L, 6L, 0L, 17L,
        0L, 17L, 0L, 16L, 0L, 16L, 0L, 16L);
    valid(expect);
    strategy.assign(23, 1, 1, tags);
    expect = Lists.newArrayList(5L, 7L, 5L, 7L, 5L, 7L, 4L, 6L, 4L, 6L, 0L, 17L,
        0L, 17L, 0L, 16L, 0L, 16L, 0L, 16L);
    valid(expect);
    strategy.assign(11, 1, 3, tags);
    expect = Lists.newArrayList(5L, 7L, 5L, 7L, 5L, 7L, 4L, 13L, 4L, 13L, 7L, 17L,
        6L, 17L, 6L, 16L, 0L, 16L, 0L, 16L);
    valid(expect);
  }

  private void valid(List<Long> expect) {
    assertEquals(20, expect.size());
    int i = 0;
    List<ServerNode> list = clusterManager.getServerList(tags);
    list.sort(new Comparator<ServerNode>() {
      @Override
      public int compare(ServerNode o1, ServerNode o2) {
        return o1.getId().compareTo(o2.getId());
      }
    });
    for (ServerNode node : list) {
      assertEquals(expect.get(i).intValue(), strategy.getServerToPartitions().get(node).getPartitionNum());
      i++;
    }
  }

  @AfterEach
  public void tearDown() {
    clusterManager.clear();
  }

  void updateServerResource(List<Long> resources) {
    for (int i = 0; i < 20; i++) {
      ServerNode node = new ServerNode(
          String.valueOf((char)('a' + i)),
          "",
          0,
          10L,
          5L,
          resources.get(i),
          5,
          tags,
          true);
      clusterManager.add(node);
    }
  }
}