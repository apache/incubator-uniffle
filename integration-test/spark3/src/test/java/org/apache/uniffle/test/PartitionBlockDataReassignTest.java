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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_BLOCK_SEND_FAILURE_RETRY_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** This class is to test the mechanism of partition block data reassignment. */
public class PartitionBlockDataReassignTest extends SparkSQLTest {

  private static String basePath;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    // for coordinator
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    // for shuffle-server
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();

    ShuffleServerConf grpcShuffleServerConf1 = buildShuffleServerConf(ServerType.GRPC);
    createShuffleServer(grpcShuffleServerConf1);

    ShuffleServerConf grpcShuffleServerConf2 = buildShuffleServerConf(ServerType.GRPC);
    createShuffleServer(grpcShuffleServerConf2);

    ShuffleServerConf grpcShuffleServerConf3 = buildShuffleServerConf(ServerType.GRPC_NETTY);
    createShuffleServer(grpcShuffleServerConf3);

    ShuffleServerConf grpcShuffleServerConf4 = buildShuffleServerConf(ServerType.GRPC_NETTY);
    createShuffleServer(grpcShuffleServerConf4);

    startServers();

    // simulate one server without enough buffer
    ShuffleServer faultyShuffleServer = grpcShuffleServers.get(0);
    ShuffleBufferManager bufferManager = faultyShuffleServer.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() + 100);
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType) throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    return shuffleServerConf;
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set("spark." + RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, "1");
    sparkConf.set("spark." + RSS_CLIENT_BLOCK_SEND_FAILURE_RETRY_ENABLED.key(), "true");
  }

  @Override
  public void checkShuffleData() throws Exception {
    Thread.sleep(12000);
    String[] paths = basePath.split(",");
    for (String path : paths) {
      File f = new File(path);
      assertEquals(0, f.list().length);
    }
  }
}
