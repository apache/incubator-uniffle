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

package com.tencent.rss.coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.metrics.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CoordinatorMetricsTest {

  private static final String SERVER_METRICS_URL = "http://127.0.0.1:12345/metrics/server";
  private static final String SERVER_JVM_URL = "http://127.0.0.1:12345/metrics/jvm";
  private static final String SERVER_GRPC_URL = "http://127.0.0.1:12345/metrics/grpc";
  private static CoordinatorServer coordinatorServer;

  @BeforeClass
  public static void setUp() throws Exception {
    String remotePath1 = "hdfs://path1";
    File cfgFile = Files.createTempFile("coordinatorMetricsTest", ".conf").toFile();
    cfgFile.deleteOnExit();
    writeRemoteStorageConf(cfgFile, remotePath1);

    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, 10);
    coordinatorConf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    coordinatorConf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    coordinatorConf.set(RssBaseConf.JETTY_HTTP_PORT, 12345);
    coordinatorConf.set(RssBaseConf.JETTY_CORE_POOL_SIZE, 128);
    coordinatorConf.set(RssBaseConf.RPC_SERVER_PORT, 12346);
    coordinatorServer = new CoordinatorServer(coordinatorConf);
    coordinatorServer.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    coordinatorServer.stopServer();
  }

  @Test
  public void testDynamicMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode metricsNode = mapper.readTree(content).get("metrics");
    String remoteStorageMetricsName = CoordinatorMetrics.REMOTE_STORAGE_IN_USED_PREFIX + "path1";
    boolean bingo = false;
    for (int i = 0; i < metricsNode.size(); i++) {
      JsonNode metricsName = metricsNode.get(i).get("name");
      if (remoteStorageMetricsName.equals(metricsName.textValue())) {
        bingo = true;
        break;
      }
    }
    assertTrue(bingo);
  }

  @Test
  public void testCoordinatorMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(8, actualObj.get("metrics").size());
  }

  @Test
  public void testJvmMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_JVM_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
  }

  @Test
  public void testGrpcMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_GRPC_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(6, actualObj.get("metrics").size());
  }

  static private void writeRemoteStorageConf(File cfgFile, String value) throws Exception {
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key() + " " + value);
    printWriter.flush();
    printWriter.close();
  }
}
