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
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategyFactory;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY;

/** This class is to test the partition reassign mechanism of load balance for huge partition. */
public class PartitionBlockDataReassignLoadBalanceTest extends PartitionBlockDataReassignBasicTest {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    // for coordinator
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    coordinatorConf.set(
        COORDINATOR_ASSIGNMENT_STRATEGY, AssignmentStrategyFactory.StrategyName.BASIC);

    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    // for shuffle-server
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();

    // grpc server.
    ShuffleServerConf grpcShuffleServerConf1 = buildShuffleServerConf(ServerType.GRPC);
    createMockedShuffleServer(grpcShuffleServerConf1);

    ShuffleServerConf grpcShuffleServerConf2 = buildShuffleServerConf(ServerType.GRPC);
    createMockedShuffleServer(grpcShuffleServerConf2);

    ShuffleServerConf grpcShuffleServerConf3 = buildShuffleServerConf(ServerType.GRPC);
    createMockedShuffleServer(grpcShuffleServerConf3);

    // netty server.
    ShuffleServerConf grpcShuffleServerConf4 = buildShuffleServerConf(ServerType.GRPC_NETTY);
    createShuffleServer(grpcShuffleServerConf4);

    ShuffleServerConf grpcShuffleServerConf5 = buildShuffleServerConf(ServerType.GRPC_NETTY);
    createShuffleServer(grpcShuffleServerConf5);

    startServers();

    // This will make the partition reassign to 2 servers.
    ShuffleServer g1 = grpcShuffleServers.get(0);
    ((MockedGrpcServer) g1.getServer())
        .getService()
        .enableMockRequireBufferFailWithNoBufferForHugePartition();
  }
}
