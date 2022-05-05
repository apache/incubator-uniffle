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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.AfterClass;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.CoordinatorServer;
import com.tencent.rss.server.MockedShuffleServer;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.util.StorageType;

abstract public class IntegrationTestBase extends HdfsTestBase {

  protected static final int SHUFFLE_SERVER_PORT = 20001;
  protected static final String LOCALHOST = "127.0.0.1";
  protected static final int COORDINATOR_PORT_1 = 19999;
  protected static final int COORDINATOR_PORT_2 = 20030;
  protected static final int JETTY_PORT_1 = 19998;
  protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT_1;

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
    coordinatorConf.setInteger(CoordinatorConf.RPC_SERVER_PORT, COORDINATOR_PORT_1);
    coordinatorConf.setInteger(CoordinatorConf.JETTY_HTTP_PORT, JETTY_PORT_1);
    coordinatorConf.setInteger(CoordinatorConf.RPC_EXECUTOR_SIZE, 10);
    return coordinatorConf;
  }

  protected static void addDynamicConf(
      CoordinatorConf coordinatorConf, Map<String, String> dynamicConf) throws Exception {
    File file = createDynamicConfFile(dynamicConf);
    coordinatorConf.setBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH,
        file.getAbsolutePath());
    coordinatorConf.setInteger(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, 5);
  }

  protected static ShuffleServerConf getShuffleServerConf() throws Exception {
    File dataFolder = Files.createTempDirectory("rssdata").toFile();
    ShuffleServerConf serverConf = new ShuffleServerConf();
    dataFolder.deleteOnExit();
    serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
    serverConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE_HDFS.name());
    serverConf.setString("rss.storage.basePath", dataFolder.getAbsolutePath());
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
    serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.coordinator.quorum", COORDINATOR_QUORUM);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 18080);
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setString("rss.server.uploader.base.path", "test");
    serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    return serverConf;
  }

  protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    coordinators.add(new CoordinatorServer(coordinatorConf));
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new ShuffleServer(serverConf));
  }

  protected static void createMockedShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new MockedShuffleServer(serverConf));
  }

  protected static void createAndStartServers(
      ShuffleServerConf shuffleServerConf,
      CoordinatorConf coordinatorConf) throws Exception {
    createCoordinatorServer(coordinatorConf);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  protected static File createDynamicConfFile(Map<String, String> dynamicConf) throws Exception {
    File dynamicConfFile = Files.createTempFile("dynamicConf", "conf").toFile();
    writeRemoteStorageConf(dynamicConfFile, dynamicConf);
    return dynamicConfFile;
  }

  protected static void writeRemoteStorageConf(
      File cfgFile, Map<String, String> dynamicConf) throws Exception {
    // sleep 2 secs to make sure the modified time will be updated
    Thread.sleep(2000);
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    for (Map.Entry<String, String> entry : dynamicConf.entrySet()) {
      printWriter.println(entry.getKey() + " " + entry.getValue());
    }
    printWriter.flush();
    printWriter.close();
  }
}