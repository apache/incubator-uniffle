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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.storage.HadoopTestBase;

public abstract class RiffleIntegrationTestBase extends HadoopTestBase {

  protected static final int SHUFFLE_SERVER_PORT = 19999;

  protected static final String LOCALHOST;

  protected static final int COORDINATOR_PORT = 9999;

  protected static final int JETTY_PORT = 9998;

  private static final Logger LOG = LoggerFactory.getLogger(RiffleIntegrationTestBase.class);

  protected static List<RiffleShuffleServer> shuffleServers = Lists.newArrayList();

  protected static List<CoordinatorServer> coordinators = Lists.newArrayList();

  static @TempDir File tempDir;

  static {
    try {
      LOCALHOST = RssUtils.getHostIp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT;

  public static void startServers() throws Exception {
    RiffleShuffleServer.compileRustServer();
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.start();
    }
    for (RiffleShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.start();
    }
  }

  @AfterAll
  public static void shutdownServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.stopServer();
    }
    for (RiffleShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    coordinators = Lists.newArrayList();
    CoordinatorMetrics.clear();
  }

  protected static RiffleShuffleServerConf getShuffleServerConf() throws Exception {
    Map<String, Object> hybridStore = new HashMap<>();
    hybridStore.put("memory_spill_high_watermark", 0.9);
    hybridStore.put("memory_spill_low_watermark", 0.5);

    Map<String, Object> memoryStore = new HashMap<>();
    memoryStore.put("capacity", "50MB");

    RiffleShuffleServerConf serverConf = new RiffleShuffleServerConf(tempDir);
    serverConf.set("coordinator_quorum", Lists.newArrayList(COORDINATOR_QUORUM));
    serverConf.set("hybrid_store", hybridStore);
    serverConf.set("memory_store", memoryStore);
    return serverConf;
  }

  protected static CoordinatorConf getCoordinatorConf() {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger(CoordinatorConf.RPC_SERVER_PORT, COORDINATOR_PORT);
    coordinatorConf.setInteger(CoordinatorConf.JETTY_HTTP_PORT, JETTY_PORT);
    coordinatorConf.setInteger(CoordinatorConf.RPC_EXECUTOR_SIZE, 10);
    return coordinatorConf;
  }

  protected static void createShuffleServer(RiffleShuffleServerConf serverConf) throws Exception {
    serverConf.generateTomlConf();
    shuffleServers.add(new RiffleShuffleServer(serverConf));
  }

  protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    coordinators.add(new CoordinatorServer(coordinatorConf));
  }

  protected static void createAndStartServers(RiffleShuffleServerConf shuffleServerConf)
      throws Exception {
    createShuffleServer(shuffleServerConf);
    startServers();
  }
}
