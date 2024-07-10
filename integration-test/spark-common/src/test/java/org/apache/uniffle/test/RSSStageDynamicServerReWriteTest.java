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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.spark.shuffle.RssSparkConfig;
import org.apache.uniffle.storage.util.StorageType;

public class RSSStageDynamicServerReWriteTest extends SparkTaskFailureIntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(RSSStageDynamicServerReWriteTest.class);

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);
    createServer(0, tmpDir, true, ServerType.GRPC);
    createServer(1, tmpDir, false, ServerType.GRPC);
    createServer(2, tmpDir, false, ServerType.GRPC);
    createServer(3, tmpDir, true, ServerType.GRPC_NETTY);
    createServer(4, tmpDir, false, ServerType.GRPC_NETTY);
    createServer(5, tmpDir, false, ServerType.GRPC_NETTY);
    startServers();
  }

  public static void createServer(int id, File tmpDir, boolean abnormalFlag, ServerType serverType)
      throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 8000);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    File dataDir1 = new File(tmpDir, id + "_1");
    File dataDir2 = new File(tmpDir, id + "_2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setInteger(
        "rss.rpc.server.port",
        shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT) + id);
    shuffleServerConf.setInteger("rss.jetty.http.port", 19081 + id * 100);
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    if (abnormalFlag) {
      createMockedShuffleServer(shuffleServerConf);
      // Set the sending block data timeout for the first shuffleServer
      switch (serverType) {
        case GRPC:
          ((MockedGrpcServer) grpcShuffleServers.get(0).getServer())
              .getService()
              .enableMockSendDataFailed(true);
          break;
        case GRPC_NETTY:
          ((MockedGrpcServer) nettyShuffleServers.get(0).getServer())
              .getService()
              .enableMockSendDataFailed(true);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported server type " + serverType);
      }
    } else {
      createShuffleServer(shuffleServerConf);
    }
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    List<Row> rows =
        spark.range(0, 1000, 1, 4).repartition(2).groupBy("id").count().collectAsList();
    Map<String, Long> result = Maps.newHashMap();
    for (Row row : rows) {
      result.put(row.get(0).toString(), row.getLong(1));
    }
    return result;
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    super.updateSparkConfCustomer(sparkConf);
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_RESUBMIT_STAGE, "true");
  }

  @Test
  public void testRSSStageResubmit() throws Exception {
    run();
  }
}
