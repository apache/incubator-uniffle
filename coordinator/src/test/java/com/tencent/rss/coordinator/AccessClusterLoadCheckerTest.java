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

package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessClusterLoadCheckerTest {

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void test() throws Exception {
    ClusterManager clusterManager = mock(SimpleClusterManager.class);
    List<ServerNode> serverNodeList = Lists.newArrayList();
    ServerNode node1 = new ServerNode(
        "1",
        "1",
        0,
        50,
        20,
        30,
        0,
        null,
        false);
    serverNodeList.add(node1);
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        "com.tencent.rss.coordinator.AccessClusterLoadChecker");
    AccessManager accessManager = new AccessManager(conf, clusterManager, new Configuration());
    AccessClusterLoadChecker accessClusterLoadChecker =
        (AccessClusterLoadChecker) accessManager.getAccessCheckers().get(0);
    when(clusterManager.getServerList(any())).thenReturn(serverNodeList);
    assertFalse(accessClusterLoadChecker.check(new AccessInfo("test")).isSuccess());
    assertEquals(2, accessClusterLoadChecker.getAvailableServerNumThreshold());
    assertEquals(0, Double.compare(accessClusterLoadChecker.getMemoryPercentThreshold(), 20.0));
    ServerNode node2 = new ServerNode(
        "1",
        "1",
        0,
        90,
        40,
        10,
        0,
        null,
        true);
    serverNodeList.add(node2);
    ServerNode node3 = new ServerNode(
        "1",
        "1",
        0,
        80,
        25,
        20,
        0,
        null,
        true);
    serverNodeList.add(node3);
    assertFalse(accessClusterLoadChecker.check(new AccessInfo("test")).isSuccess());
    ServerNode node4 = new ServerNode(
        "1",
        "1",
        0,
        75,
        25,
        25,
        0,
        null,
        true);
    serverNodeList.add(node4);
    assertTrue(accessClusterLoadChecker.check(new AccessInfo("test")).isSuccess());
  }
}
