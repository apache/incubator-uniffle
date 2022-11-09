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

package org.apache.uniffle.server.state;

import java.io.File;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KryoSerializationStateStoreTest {

  @Test
  public void testIllegalArgs() {
    try {
      new KryoSerializationStateStore("");
      fail();
    } catch (Exception e) {
      // ignore
    }

    try {
      new KryoSerializationStateStore(null);
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void test(@TempDir File tempDir) throws Exception {
    String location = tempDir.getAbsolutePath() + "/state.bin";
    StateStore stateStore = new KryoSerializationStateStore(location);

    Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds = Maps.newConcurrentMap();
    partitionsToBlockIds.putIfAbsent("testAppId", Maps.newConcurrentMap());
    partitionsToBlockIds.get("testAppId").put(1,
        new Roaring64NavigableMap[] {Roaring64NavigableMap.bitmapOf(100, 1001, 10001)});

    ShuffleServerState state = ShuffleServerState.builder()
        .inFlushSize(1000)
        .readDataMemory(2000)
        .partitionsToBlockIds(partitionsToBlockIds)
        .build();
    stateStore.export(state);

    ShuffleServerState recoverableState = stateStore.restore();
    assertEquals(state.getInFlushSize(), recoverableState.getInFlushSize());
    assertEquals(state.getReadDataMemory(), recoverableState.getReadDataMemory());
    assertTrue(state.getPartitionsToBlockIds().get("testAppId").get(1)[0].contains(1001));
  }
}
