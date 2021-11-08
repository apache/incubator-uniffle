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

package com.tencent.rss.test;

import com.google.common.collect.Sets;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitionBalanceCoordinatorGrpcTest extends IntegrationTestBase {

  private CoordinatorClientFactory factory = new CoordinatorClientFactory("GRPC");
  private CoordinatorGrpcClient coordinatorClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 30000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setString("rss.server.buffer.capacity", "204800000");
    createShuffleServer(shuffleServerConf);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    shuffleServerConf.setString("rss.server.buffer.capacity", "512000000");
    createShuffleServer(shuffleServerConf);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 2);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18082);
    shuffleServerConf.setString("rss.server.buffer.capacity", "102400000");
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    coordinatorClient =
        (CoordinatorGrpcClient) factory.createCoordinatorClient(LOCALHOST, COORDINATOR_PORT_1);
  }

  @After
  public void closeClient() {
    if (coordinatorClient != null) {
      coordinatorClient.close();
    }
  }

  @Test
  public void getShuffleAssignmentsTest() throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 3);
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest(
        "app1",
        1,
        1,
        1,
        1,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(SHUFFLE_SERVER_PORT + 1, entry.getValue().get(0).getPort());
    }
    request = new RssGetShuffleAssignmentsRequest(
        "app1",
        2,
        1,
        1,
        1,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(SHUFFLE_SERVER_PORT + 1, entry.getValue().get(0).getPort());
    }
    request = new RssGetShuffleAssignmentsRequest(
        "app1",
        2,
        1,
        1,
        1,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(SHUFFLE_SERVER_PORT, entry.getValue().get(0).getPort());
    }
  }
}
