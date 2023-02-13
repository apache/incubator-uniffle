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

package org.apache.uniffle.test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.TestUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServletTest extends IntegrationTestBase {
  private static final String URL_PREFIX = "http://127.0.0.1:12345/api/";
  private static final String NODES_URL = URL_PREFIX + "server/nodes";
  private static final String DECOMMISSION_URL = URL_PREFIX + "server/decommission";
  private static CoordinatorServer coordinatorServer;
  private ObjectMapper objectMapper = new ObjectMapper();

  @BeforeAll
  public static void setUp(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.set(RssBaseConf.JETTY_HTTP_PORT, 12345);
    coordinatorConf.set(RssBaseConf.JETTY_CORE_POOL_SIZE, 128);
    coordinatorConf.set(RssBaseConf.RPC_SERVER_PORT, 12346);
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.set(RssBaseConf.RSS_COORDINATOR_QUORUM, "127.0.0.1:12346");
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    List<String> basePath = Lists.newArrayList(dataDir1.getAbsolutePath(), dataDir2.getAbsolutePath());
    shuffleServerConf.setString(RssBaseConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    shuffleServerConf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, basePath);
    createShuffleServer(shuffleServerConf);
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = Lists.newArrayList(dataDir3.getAbsolutePath(), dataDir4.getAbsolutePath());
    shuffleServerConf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, basePath);
    shuffleServerConf.set(RssBaseConf.RPC_SERVER_PORT, SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.set(RssBaseConf.JETTY_HTTP_PORT, 18081);
    createShuffleServer(shuffleServerConf);
    startServers();
    coordinatorServer = coordinators.get(0);
    Awaitility.await().timeout(30, TimeUnit.SECONDS).until(() ->
        coordinatorServer.getClusterManager().list().size() == 2);
  }

  @Test
  public void testNodesServlet() throws Exception {
    String content = TestUtils.httpGet(NODES_URL);
    Response<List<HashMap>> response = objectMapper.readValue(content, new TypeReference<Response<List<HashMap>>>() {
    });
    List<HashMap> serverList = response.getData();
    assertEquals(0, response.getCode());
    assertEquals(2, serverList.size());
    assertEquals(SHUFFLE_SERVER_PORT, Integer.parseInt(serverList.get(0).get("port").toString()));
    assertEquals(ServerStatus.NORMAL_STATUS.toString(), serverList.get(0).get("status"));
    assertEquals(SHUFFLE_SERVER_PORT + 1, Integer.parseInt(serverList.get(1).get("port").toString()));
    assertEquals(ServerStatus.NORMAL_STATUS.toString(), serverList.get(1).get("status"));

    // Only fetch one server.
    ShuffleServer shuffleServer = shuffleServers.get(0);
    content = TestUtils.httpGet(NODES_URL + "?id=" + shuffleServer.getId());
    response = objectMapper.readValue(content, new TypeReference<Response<List<HashMap>>>() {
    });
    serverList = response.getData();
    assertEquals(1, serverList.size());
    assertEquals(shuffleServer.getId(), serverList.get(0).get("id"));
  }

  @Test
  public void testDecommissionServlet() throws Exception {
    ShuffleServer shuffleServer = shuffleServers.get(0);
    assertEquals(ServerStatus.NORMAL_STATUS, shuffleServer.getServerStatus());
    Map<String, Object> params = new HashMap<>();
    params.put("serverId", shuffleServer.getId());
    params.put("on", false);
    String content = TestUtils.httpPost(DECOMMISSION_URL, objectMapper.writeValueAsString(params));
    Response response = objectMapper.readValue(content, Response.class);
    assertEquals(-1, response.getCode());
    assertNotNull(response.getErrMsg());
    assertTrue(response.getErrMsg().contains("is not decommissioning. Nothing need to do"));

    // Register shuffle, avoid server exiting immediately.
    ShuffleServerGrpcClient shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("testDecommissionServlet_appId", 0,
        Lists.newArrayList(new PartitionRange(0, 1)), ""));
    params.put("on", true);
    content = TestUtils.httpPost(DECOMMISSION_URL, objectMapper.writeValueAsString(params));
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());

    // Wait until shuffle server send heartbeat to coordinator.
    Awaitility.await().timeout(10, TimeUnit.SECONDS).until(() ->
        ServerStatus.DECOMMISSIONING.equals(
            coordinatorServer.getClusterManager().getServerNodeById(shuffleServer.getId()).getStatus()));
    // Cancel decommission.
    params.put("on", false);
    content = TestUtils.httpPost(DECOMMISSION_URL, objectMapper.writeValueAsString(params));
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.NORMAL_STATUS, shuffleServer.getServerStatus());
  }
}
