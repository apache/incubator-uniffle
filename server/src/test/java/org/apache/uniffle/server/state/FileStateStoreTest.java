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
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.server.buffer.ShuffleBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FileStateStoreTest {

  @Test
  public void testIllegalArgs() {
    try {
      new FileStateStore("");
      fail();
    } catch (Exception e) {
      // ignore
    }

    try {
      new FileStateStore(null);
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void test(@TempDir File tempDir) throws Exception {
    Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds = Maps.newConcurrentMap();
    partitionsToBlockIds.putIfAbsent("testAppId", Maps.newConcurrentMap());
    partitionsToBlockIds.get("testAppId").put(1,
        new Roaring64NavigableMap[] {Roaring64NavigableMap.bitmapOf(100, 1001, 10001)});

    RangeMap<Integer, ShuffleBuffer> rangeMap = TreeRangeMap.create();
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(1000);
    rangeMap.put(Range.closed(1, 10), shuffleBuffer);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = Maps.newConcurrentMap();
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> entry = Maps.newConcurrentMap();
    entry.put(1, rangeMap);
    bufferPool.put("hello", entry);

    ShuffleServerState state = ShuffleServerState.builder()
        .inFlushSize(1000)
        .readDataMemory(2000)
        .partitionsToBlockIds(partitionsToBlockIds)
        .bufferPool(bufferPool)
        .build();

    String location = tempDir.getAbsolutePath() + "/state.bin";
    StateStore stateStore = new FileStateStore(location);
    stateStore.export(state);

    ShuffleServerState recoverableState = stateStore.restore();
    assertEquals(state.getInFlushSize(), recoverableState.getInFlushSize());
    assertEquals(state.getReadDataMemory(), recoverableState.getReadDataMemory());
    assertTrue(state.getPartitionsToBlockIds().get("testAppId").get(1)[0].contains(1001));

    assertTrue(state.getBufferPool().get("hello").get(1).get(2).getInFlushBlockMap().isEmpty());
  }
}
