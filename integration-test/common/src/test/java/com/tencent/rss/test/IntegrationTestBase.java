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

import com.google.common.collect.Lists;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.CoordinatorServer;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.util.StorageType;

import java.util.List;
import org.junit.AfterClass;

abstract public class IntegrationTestBase extends HdfsTestBase {

  protected static final int SHUFFLE_SERVER_PORT = 20001;
  protected static final String LOCALHOST = "127.0.0.1";
  protected static final int COORDINATOR_PORT_1 = 19999;
  protected static final int COORDINATOR_PORT_2 = 20030;
  protected static final int JETTY_PORT_1 = 19998;
  protected static final int JETTY_PORT_2 = 20032;
  protected static final String COORDINATOR_QUORUM =
      LOCALHOST + ":" + COORDINATOR_PORT_1 + "," + LOCALHOST + ":" + COORDINATOR_PORT_2;

  protected static List<ShuffleServer> shuffleServers = Lists.newArrayList();
  protected static List<CoordinatorServer> coordinators = Lists.newArrayList();

  public static void startServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.start();
    }
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.start();
    }
  }

  @AfterClass
  public static void shutdownServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.stopServer();
    }
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    coordinators = Lists.newArrayList();
  }

  protected static CoordinatorConf getCoordinatorConf() {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger("rss.rpc.server.port", COORDINATOR_PORT_1);
    coordinatorConf.setInteger("rss.jetty.http.port", JETTY_PORT_1);
    coordinatorConf.setInteger("rss.rpc.executor.size", 10);
    return coordinatorConf;
  }

  protected static ShuffleServerConf getShuffleServerConf() {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
    serverConf.setString("rss.storage.type", StorageType.HDFS.name());
    serverConf.setString("rss.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.buffer.spill.threshold", "335544320");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.server.partition.buffer.size", "67108864");
    serverConf.setString("rss.coordinator.quorum", COORDINATOR_QUORUM);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 18080);
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setString("rss.server.uploader.base.path", "test");
    serverConf.setLong("rss.server.disk.capacity", 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    return serverConf;
  }

  protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    coordinators.add(new CoordinatorServer(coordinatorConf));
    coordinatorConf.setInteger("rss.rpc.server.port", COORDINATOR_PORT_2);
    coordinatorConf.setInteger("rss.jetty.http.port", JETTY_PORT_2);
    coordinators.add(new CoordinatorServer(coordinatorConf));
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new ShuffleServer(serverConf));
  }

  protected static void createAndStartServers(
      ShuffleServerConf shuffleServerConf,
      CoordinatorConf coordinatorConf) throws Exception {
    createCoordinatorServer(coordinatorConf);
    createShuffleServer(shuffleServerConf);
    startServers();
  }
}