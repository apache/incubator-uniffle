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

package com.tencent.rss.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.ServerNode;
import com.tencent.rss.coordinator.SimpleClusterManager;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoordinatorGrpcTest extends IntegrationTestBase {

  private CoordinatorClientFactory factory = new CoordinatorClientFactory("GRPC");
  private CoordinatorGrpcClient coordinatorClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 3000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
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
  public void testGetPartitionToServers() {
    GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        coordinatorClient.getPartitionToServers(testResponse);

    assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
        new ShuffleServerInfo("id2", "0.0.0.2", 100)),
        partitionToServers.get(0));
    assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
        new ShuffleServerInfo("id2", "0.0.0.2", 100)),
        partitionToServers.get(1));
    assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
        new ShuffleServerInfo("id4", "0.0.0.4", 100)),
        partitionToServers.get(2));
    assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
        new ShuffleServerInfo("id4", "0.0.0.4", 100)),
        partitionToServers.get(3));
    assertNull(partitionToServers.get(4));
  }

  @Test
  public void getShuffleRegisterInfoTest() {
    GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
        coordinatorClient.getServerToPartitionRanges(testResponse);
    List<ShuffleRegisterInfo> expected = Arrays.asList(
        new ShuffleRegisterInfo(new ShuffleServerInfo("id1", "0.0.0.1", 100),
            Lists.newArrayList(new PartitionRange(0, 1))),
        new ShuffleRegisterInfo(new ShuffleServerInfo("id2", "0.0.0.2", 100),
            Lists.newArrayList(new PartitionRange(0, 1))),
        new ShuffleRegisterInfo(new ShuffleServerInfo("id3", "0.0.0.3", 100),
            Lists.newArrayList(new PartitionRange(2, 3))),
        new ShuffleRegisterInfo(new ShuffleServerInfo("id4", "0.0.0.4", 100),
            Lists.newArrayList(new PartitionRange(2, 3))));
    assertEquals(4, serverToPartitionRanges.size());
    for (ShuffleRegisterInfo sri : expected) {
      List<PartitionRange> partitionRanges = serverToPartitionRanges.get(sri.getShuffleServerInfo());
      assertEquals(sri.getPartitionRanges(), partitionRanges);
    }
  }

  @Test
  public void getShuffleAssignmentsTest() throws Exception {
    String appId = "getShuffleAssignmentsTest";
    waitForRegister(2);
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest(
        appId, 1, 10, 4, 1,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    Set<Integer> expectedStart = Sets.newHashSet(0, 4, 8);

    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = response.getServerToPartitionRanges();
    assertEquals(2, serverToPartitionRanges.size());
    List<PartitionRange> partitionRanges = Lists.newArrayList();
    for (List<PartitionRange> ranges : serverToPartitionRanges.values()) {
      partitionRanges.addAll(ranges);
    }
    for (PartitionRange pr : partitionRanges) {
      switch (pr.getStart()) {
        case 0:
          assertEquals(3, pr.getEnd());
          expectedStart.remove(0);
          break;
        case 4:
          assertEquals(7, pr.getEnd());
          expectedStart.remove(4);
          break;
        case 8:
          assertEquals(11, pr.getEnd());
          expectedStart.remove(8);
          break;
        default:
          fail("Shouldn't be here");
      }
    }
    assertTrue(expectedStart.isEmpty());

    request = new RssGetShuffleAssignmentsRequest(
        appId, 1, 10, 4, 2,
        Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    response = coordinatorClient.getShuffleAssignments(request);
    serverToPartitionRanges = response.getServerToPartitionRanges();
    assertEquals(2, serverToPartitionRanges.size());
    partitionRanges = Lists.newArrayList();
    for (List<PartitionRange> ranges : serverToPartitionRanges.values()) {
      partitionRanges.addAll(ranges);
    }
    assertEquals(6, partitionRanges.size());
    int range0To3 = 0;
    int range4To7 = 0;
    int range8To11 = 0;
    for (PartitionRange pr : partitionRanges) {
      switch (pr.getStart()) {
        case 0:
          assertEquals(3, pr.getEnd());
          range0To3++;
          break;
        case 4:
          assertEquals(7, pr.getEnd());
          range4To7++;
          break;
        case 8:
          assertEquals(11, pr.getEnd());
          range8To11++;
          break;
        default:
          fail("Shouldn't be here");
      }
    }
    assertEquals(2, range0To3);
    assertEquals(2, range4To7);
    assertEquals(2, range8To11);

    request = new RssGetShuffleAssignmentsRequest(
        appId, 3, 2, 1, 1,
        Sets.newHashSet("fake_version"));
    try {
      coordinatorClient.getShuffleAssignments(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Empty assignment"));
    }
  }

  @Test
  public void appHeartbeatTest() throws Exception {
    RssAppHeartBeatResponse response =
        coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest1", 1000));
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    assertEquals(Sets.newHashSet("appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest2", 1000));
    assertEquals(Sets.newHashSet("appHeartbeatTest1", "appHeartbeatTest2"),
        coordinators.get(0).getApplicationManager().getAppIds());
    int retry = 0;
    while (retry < 5) {
      coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest1", 1000));
      retry++;
      Thread.sleep(1000);
    }
    // appHeartbeatTest2 was removed because of expired
    assertEquals(Sets.newHashSet("appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
  }

  @Test
  public void shuffleServerHeartbeatTest() throws Exception {
    waitForRegister(2);
    shuffleServers.get(0).stopServer();
    Thread.sleep(5000);
    SimpleClusterManager scm = (SimpleClusterManager) coordinators.get(0).getClusterManager();
    List<ServerNode> nodes = scm.getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    assertEquals(1, nodes.size());
    ServerNode node = nodes.get(0);
    assertTrue(node.getTags().contains(Constants.SHUFFLE_SERVER_VERSION));
    assertTrue(scm.getTagToNodes().get(Constants.SHUFFLE_SERVER_VERSION).contains(node));
    ShuffleServerConf shuffleServerConf = shuffleServers.get(0).getShuffleServerConf();
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 2);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18082);
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    ss.start();
    shuffleServers.set(0, ss);
    Thread.sleep(3000);
    assertEquals(2, coordinators.get(0).getClusterManager().getNodesNum());
  }

  private void waitForRegister(int expcetedServers) throws Exception {
    GetShuffleServerListResponse response;
    int count = 0;
    do {
      response = coordinatorClient.getShuffleServerList();
      Thread.sleep(1000);
      if (count > 10) {
        throw new RuntimeException("No shuffle server connected");
      }
      count++;
    } while (response.getServersCount() < expcetedServers);
  }

  private GetShuffleAssignmentsResponse generateShuffleAssignmentsResponse() {
    ShuffleServerId ss1 = RssProtos.ShuffleServerId.newBuilder()
        .setIp("0.0.0.1")
        .setPort(100)
        .setId("id1")
        .build();

    ShuffleServerId ss2 = RssProtos.ShuffleServerId.newBuilder()
        .setIp("0.0.0.2")
        .setPort(100)
        .setId("id2")
        .build();

    ShuffleServerId ss3 = RssProtos.ShuffleServerId.newBuilder()
        .setIp("0.0.0.3")
        .setPort(100)
        .setId("id3")
        .build();

    ShuffleServerId ss4 = RssProtos.ShuffleServerId.newBuilder()
        .setIp("0.0.0.4")
        .setPort(100)
        .setId("id4")
        .build();

    PartitionRangeAssignment assignment1 =
        RssProtos.PartitionRangeAssignment.newBuilder()
            .setStartPartition(0)
            .setEndPartition(1)
            .addAllServer(Arrays.asList(ss1, ss2))
            .build();

    PartitionRangeAssignment assignment2 =
        RssProtos.PartitionRangeAssignment.newBuilder()
            .setStartPartition(2)
            .setEndPartition(3)
            .addAllServer(Arrays.asList(ss3, ss4))
            .build();

    return RssProtos.GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(Arrays.asList(assignment1, assignment2))
        .build();
  }
}
