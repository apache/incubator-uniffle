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
import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

public class RepartitionWithMemoryRssTest extends RepartitionTest {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, 5000L);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String grpcBasePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, grpcBasePath);

    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    String nettyBasePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, nettyBasePath);
    createShuffleServer(grpcShuffleServerConf);
    createShuffleServer(nettyShuffleServerConf);
    startServers();
  }

  @Test
  public void testMemoryRelease() throws Exception {
    final String fileName = generateTextFile(10000, 10000);
    SparkConf sparkConf = createSparkConf();
    updateSparkConfWithRss(sparkConf);
    sparkConf.set("spark.executor.memory", "500m");
    sparkConf.set("spark.unsafe.exceptionOnMemoryLeak", "true");
    updateRssStorage(sparkConf);

    // oom if there has no memory release
    runSparkApp(sparkConf, fileName);
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType grpc, String basePath)
      throws Exception {
    ShuffleServerConf grpcShuffleServerConf = getShuffleServerConf(grpc);
    grpcShuffleServerConf.set(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL, 5000L);
    grpcShuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 4000L);
    grpcShuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    grpcShuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    grpcShuffleServerConf.setString(ShuffleServerConf.SERVER_BUFFER_CAPACITY.key(), "512mb");
    return grpcShuffleServerConf;
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {}
}
