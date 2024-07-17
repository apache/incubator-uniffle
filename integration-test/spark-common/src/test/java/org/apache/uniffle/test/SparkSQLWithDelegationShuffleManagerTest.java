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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.spark.shuffle.RssSparkConfig;
import org.apache.uniffle.storage.util.StorageType;

public class SparkSQLWithDelegationShuffleManagerTest extends SparkSQLTest {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    final String candidates =
        Objects.requireNonNull(
                SparkSQLWithDelegationShuffleManagerTest.class
                    .getClassLoader()
                    .getResource("candidates"))
            .getFile();
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setString(
        CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.key(),
        "org.apache.uniffle.coordinator.access.checker.AccessCandidatesChecker,"
            + "org.apache.uniffle.coordinator.access.checker.AccessClusterLoadChecker");
    coordinatorConf.set(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH, candidates);
    coordinatorConf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, 5000L);
    coordinatorConf.set(CoordinatorConf.COORDINATOR_ACCESS_LOADCHECKER_SERVER_NUM_THRESHOLD, 1);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String grpcBasePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, grpcBasePath);
    createShuffleServer(grpcShuffleServerConf);

    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    String nettyBasePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, nettyBasePath);
    createShuffleServer(nettyShuffleServerConf);

    startServers();
    Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType, String basePath)
      throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.set(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL, 1000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 4000L);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.setString(ShuffleServerConf.SERVER_BUFFER_CAPACITY.key(), "512mb");
    return shuffleServerConf;
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_ACCESS_ID.key(), "test_access_id");
    sparkConf.set(
        "spark.shuffle.manager", "org.apache.uniffle.spark.shuffle.DelegationRssShuffleManager");
  }

  @Override
  public void checkShuffleData() throws Exception {}
}
