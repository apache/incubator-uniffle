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

import com.google.common.collect.Lists;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerInternalGrpcClient;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssDecommissionResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerInternalGrpcTest extends IntegrationTestBase {

  private ShuffleServerGrpcClient shuffleServerClient;
  private ShuffleServerInternalGrpcClient shuffleServerInternalClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File dataDir1 = new File(tmpDir, "data1");
    String basePath = dataDir1.getAbsolutePath();
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
    shuffleServerInternalClient = new ShuffleServerInternalGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @Test
  public void decommissionTest() {
    String appId = "decommissionTest";
    int shuffleId = 0;
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest(appId, shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)), ""));
    RssDecommissionResponse response = shuffleServerInternalClient.decommission(new RssDecommissionRequest(true));
    assertTrue(response.isOn());
    response = shuffleServerInternalClient.decommission(new RssDecommissionRequest(false));
    assertTrue(!response.isOn());

    // Clean all apps, shuffle server will be shutdown right now.
    shuffleServerClient.unregisterShuffle(new RssUnregisterShuffleRequest(appId, shuffleId));
    response = shuffleServerInternalClient.decommission(new RssDecommissionRequest(true));
    assertTrue(response.isOn());
    try {
      // Server is already shutdown, so exception should be thrown here.
      shuffleServerInternalClient.decommission(new RssDecommissionRequest(true));
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      //assertTrue(e.getMessage().contains("must be set by the client or fetched from coordinators"));
    }

  }

}
