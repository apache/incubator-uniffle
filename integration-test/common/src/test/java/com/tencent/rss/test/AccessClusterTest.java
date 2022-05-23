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

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.File;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.request.RssAccessClusterRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAccessClusterResponse;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccessClusterTest extends CoordinatorTestBase {

  @Test
  public void test(@TempDir File tempDir) throws Exception {
    File cfgFile = File.createTempFile("tmp", ".conf", tempDir);
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("9527");
    printWriter.println(" 135 ");
    printWriter.println("2 ");
    printWriter.flush();
    printWriter.close();

    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setInteger("rss.coordinator.access.loadChecker.serverNum.threshold", 2);
    coordinatorConf.setString("rss.coordinator.access.candidates.path", cfgFile.getAbsolutePath());
    coordinatorConf.setString(
        "rss.coordinator.access.checkers",
        "com.tencent.rss.coordinator.AccessCandidatesChecker,com.tencent.rss.coordinator.AccessClusterLoadChecker");
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();

    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
    String accessId = "111111";
    RssAccessClusterRequest request = new RssAccessClusterRequest(
        accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000);
    RssAccessClusterResponse response = coordinatorClient.accessCluster(request);
    assertEquals(ResponseStatusCode.ACCESS_DENIED, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("Denied by AccessCandidatesChecker"));

    accessId = "135";
    request = new RssAccessClusterRequest(
        accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000);
    response = coordinatorClient.accessCluster(request);
    assertEquals(ResponseStatusCode.ACCESS_DENIED, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("Denied by AccessClusterLoadChecker"));

    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 2);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18082);
    ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    CoordinatorClient client = new CoordinatorClientFactory("GRPC")
        .createCoordinatorClient(LOCALHOST, COORDINATOR_PORT_1 + 13);
    request = new RssAccessClusterRequest(
        accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000);
    response = client.accessCluster(request);
    assertEquals(ResponseStatusCode.INTERNAL_ERROR, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("UNAVAILABLE: io exception"));

    request = new RssAccessClusterRequest(
        accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000);
    response = coordinatorClient.accessCluster(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("SUCCESS"));
    shuffleServer.stopServer();
  }
}

