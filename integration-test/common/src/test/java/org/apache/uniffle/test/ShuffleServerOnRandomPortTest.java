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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerOnRandomPortTest extends CoordinatorTestBase {
  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY.key(), "BASIC");
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 3000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setInteger("rss.server.netty.port", 0);
    shuffleServerConf.setInteger("rss.rpc.server.port", 0);
    shuffleServerConf.setInteger("rss.random.port.min", 30000);
    shuffleServerConf.setInteger("rss.random.port.max", 40000);
    createShuffleServer(shuffleServerConf);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Test
  public void startStreamServerOnRandomPort() throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 2);
    Thread.sleep(5000);
    int actualPort = shuffleServers.get(0).getNettyPort();
    assertTrue(actualPort >= 30000 && actualPort < 40000);
    actualPort = shuffleServers.get(1).getNettyPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);

    int maxRetries = 100;
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    // start netty server with already bind port
    shuffleServerConf.setInteger("rss.server.netty.port", actualPort);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18082);
    shuffleServerConf.setInteger("rss.port.max.retry", maxRetries);
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    ss.start();
    assertTrue(ss.getNettyPort() > actualPort && actualPort <= actualPort + maxRetries);
    ss.stopServer();
  }

  @Test
  public void startGrpcServerOnRandomPort() throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 2);
    Thread.sleep(5000);
    int actualPort = shuffleServers.get(0).getGrpcPort();
    assertTrue(actualPort >= 30000 && actualPort < 40000);
    actualPort = shuffleServers.get(1).getGrpcPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);

    int maxRetries = 100;
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    // start grpc server with already bind port
    shuffleServerConf.setInteger("rss.rpc.server.port", actualPort);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18083);
    shuffleServerConf.setInteger("rss.port.max.retry", maxRetries);
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    ss.start();
    assertTrue(ss.getGrpcPort() > actualPort && actualPort <= actualPort + maxRetries);
    ss.stopServer();
  }
}
