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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HybridStorageHadoopFallbackTest extends HybridStorageFaultToleranceBase {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    final CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);

    String basePath = generateBasePath(tmpDir);

    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, basePath);
    createShuffleServer(grpcShuffleServerConf);

    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, basePath);
    createShuffleServer(nettyShuffleServerConf);

    startServers();

    grpcShuffleServerConfig = grpcShuffleServerConf;
    nettyShuffleServerConfig = nettyShuffleServerConf;
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType, String basePath)
      throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.setDouble(ShuffleServerConf.CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setDouble(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setLong(
        ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 60L * 1000L * 60L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 20L * 1000L);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE_HDFS.name());
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.setLong(
        ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 1L * 1024L * 1024L);
    return shuffleServerConf;
  }

  @BeforeEach
  public void createClient() throws Exception {
    ShuffleServerClientFactory.getInstance().cleanupCache();
    grpcShuffleServerClient =
        new ShuffleServerGrpcClient(
            LOCALHOST, grpcShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    nettyShuffleServerClient =
        new ShuffleServerGrpcNettyClient(
            LOCALHOST,
            nettyShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT),
            nettyShuffleServerConfig.getInteger(ShuffleServerConf.NETTY_SERVER_PORT));
  }

  @Override
  public void makeChaos() {
    assertEquals(1, cluster.getDataNodes().size());
    cluster.stopDataNode(0);
    assertEquals(0, cluster.getDataNodes().size());
  }
}
