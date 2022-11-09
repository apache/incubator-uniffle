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

package org.apache.uniffle.server;

import java.io.File;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;

import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_STATE_STORE_EXPORT_DATA_LOCATION;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatefulUpgradeManagerTest {

  @Test
  public void test(@TempDir File tmpDir) throws Exception {
    String locationPath = tmpDir.getAbsolutePath() + "/state.bin";
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(STATEFUL_UPGRADE_STATE_STORE_EXPORT_DATA_LOCATION, locationPath);

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    when(mockShuffleServer.getShuffleServerConf()).thenReturn(conf);
    ServerInterface grpcServer = new ShuffleServerFactory(mockShuffleServer).getServer();
    when(mockShuffleServer.getServer()).thenReturn(grpcServer);

    ShuffleFlushManager flushManager = mock(ShuffleFlushManager.class);
    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(flushManager);
    ShuffleTaskManager taskManager = mock(ShuffleTaskManager.class);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(taskManager);
    ShuffleBufferManager bufferManager = mock(ShuffleBufferManager.class);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(bufferManager);

    Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds = Maps.newConcurrentMap();
    partitionsToBlockIds.putIfAbsent("testAppId", Maps.newConcurrentMap());
    partitionsToBlockIds.get("testAppId").put(1,
        new Roaring64NavigableMap[] {Roaring64NavigableMap.bitmapOf(100, 1001, 10001)});
    when(taskManager.getPartitionsToBlockIds()).thenReturn(partitionsToBlockIds);

    StatefulUpgradeManager statefulUpgradeManager = new StatefulUpgradeManager(mockShuffleServer, conf);
    statefulUpgradeManager.finalizeAndMaterializeState();

    assertTrue(statefulUpgradeManager.recoverState());
    assertTrue(mockShuffleServer.getShuffleTaskManager()
        .getPartitionsToBlockIds().get("testAppId").get(1)[0].contains(10001));
  }
}
