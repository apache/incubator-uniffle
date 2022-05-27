/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import com.tencent.rss.common.PartitionRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CoordinatorUtilsTest {

  @Test
  public void testNextId() {
    assertEquals(1, CoordinatorUtils.nextIdx(0, 3));
    assertEquals(2, CoordinatorUtils.nextIdx(1, 3));
    assertEquals(0, CoordinatorUtils.nextIdx(2, 3));
  }

  @Test
  public void testGenerateRanges() {
    List<PartitionRange> ranges = CoordinatorUtils.generateRanges(16, 5);
    assertEquals(new PartitionRange(0, 4), ranges.get(0));
    assertEquals(new PartitionRange(5, 9), ranges.get(1));
    assertEquals(new PartitionRange(10, 14), ranges.get(2));
    assertEquals(new PartitionRange(15, 19), ranges.get(3));
  }

  @Test
  public void testExtractClusterConf() {
    String confStr = "h1,k1-1=v1-1,k1-2=v1-2;h2,k2-1=v2-1";
    Map<String, Map<String, String>> conf = CoordinatorUtils.extractRemoteStorageConf(confStr);
    Map<String, Map<String, String>> expectConf = Maps.newTreeMap();
    expectConf.put("h1", ImmutableMap.of("k1-1", "v1-1", "k1-2", "v1-2"));
    expectConf.put("h2", ImmutableMap.of("k2-1", "v2-1"));
    assertEquals(2, conf.size());
    compareConfMap(expectConf, conf);

    confStr = "h1,k1-1=v1-1,k1-2=v1-2;";
    conf = CoordinatorUtils.extractRemoteStorageConf(confStr);
    expectConf = Maps.newTreeMap();
    expectConf.put("h1", ImmutableMap.of("k1-1", "v1-1", "k1-2", "v1-2"));
    assertEquals(1, conf.size());
    compareConfMap(expectConf, conf);

    confStr = "h1,k1-1=v1-1,k1-2=v1-2;h1,k1-1=";
    conf = CoordinatorUtils.extractRemoteStorageConf(confStr);
    expectConf = Maps.newTreeMap();
    expectConf.put("h1", ImmutableMap.of("k1-1", "v1-1", "k1-2", "v1-2"));
    assertEquals(0, conf.size());

    confStr = "";
    conf = CoordinatorUtils.extractRemoteStorageConf(confStr);
    expectConf = Maps.newTreeMap();
    expectConf.put("h1", ImmutableMap.of("k1-1", "v1-1", "k1-2", "v1-2"));
    assertEquals(0, conf.size());
  }

  private void compareConfMap(Map<String, Map<String, String>> expect, Map<String, Map<String, String>> conf) {
    assertEquals(expect.size(), conf.size());
    assertEquals(expect.size(), conf.size());
    for (String key1 : expect.keySet()) {
      Map<String, String> expectMap = expect.get(key1);
      Map<String, String> confMap = conf.get(key1);
      assertNotNull(expectMap);
      assertNotNull(confMap);
      for (String key2 : expectMap.keySet()) {
        assertEquals(expectMap.get(key2), confMap.get(key2));
      }
    }
  }
}
