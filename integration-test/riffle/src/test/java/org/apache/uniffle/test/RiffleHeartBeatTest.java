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
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.coordinator.CoordinatorConf;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RiffleHeartBeatTest extends RiffleIntegrationTestBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    createCoordinatorServer(coordinatorConf);
    RiffleShuffleServerConf shuffleServerConf = getShuffleServerConf();
    Map<String, Object> conf = new HashMap<>();
    conf.put("grpc_port", 8888);

    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @Test
  public void appHeartBeatTest() throws InterruptedException {
    String appId = "appHeartBeatTest";
    String dataBasePath = "";
    int shuffleId = 0;

    // register app and send heart beat, should succeed
    RssRegisterShuffleResponse rssRegisterShuffleResponse =
        shuffleServerClient.registerShuffle(
            new RssRegisterShuffleRequest(
                appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), dataBasePath));
    RssAppHeartBeatResponse heartBeatResponse =
        shuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));

    assertEquals(StatusCode.SUCCESS, rssRegisterShuffleResponse.getStatusCode());
    assertEquals(StatusCode.SUCCESS, heartBeatResponse.getStatusCode());

    // unregister app and send heart beat, should also succeed
    RssUnregisterShuffleResponse rssUnregisterShuffleResponse =
        shuffleServerClient.unregisterShuffle(new RssUnregisterShuffleRequest(appId, shuffleId));
    heartBeatResponse = shuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));
    assertEquals(StatusCode.SUCCESS, rssUnregisterShuffleResponse.getStatusCode());
    assertEquals(StatusCode.SUCCESS, heartBeatResponse.getStatusCode());
  }
}
