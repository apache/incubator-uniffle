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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.impl.ComposedClientReadHandler;
import org.apache.uniffle.storage.handler.impl.MemoryQuorumClientReadHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerWithLocalOfExceptionTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient shuffleServerClient;
  private static String REMOTE_STORAGE = HDFS_URI + "rss/test";

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString("rss.server.app.expired.withoutHeartbeat", "5000");
    createShuffleServer(shuffleServerConf);

    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @AfterEach
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void testReadWhenConnectionFailed() throws Exception {
    String testAppId = "testReadWhenConnectionFailed";
    int shuffleId = 0;
    int partitionId = 0;

    MemoryQuorumClientReadHandler memoryQuorumClientReadHandler = new MemoryQuorumClientReadHandler(
        testAppId, shuffleId, partitionId, 150, Lists.newArrayList(shuffleServerClient));
    ClientReadHandler[] handlers = new ClientReadHandler[1];
    handlers[0] = memoryQuorumClientReadHandler;
    ComposedClientReadHandler composedClientReadHandler = new ComposedClientReadHandler(handlers);
    shuffleServers.get(0).stopServer();
    ShuffleDataResult sdr  = composedClientReadHandler.readShuffleData();
    assertTrue(sdr == null);
  }
}
