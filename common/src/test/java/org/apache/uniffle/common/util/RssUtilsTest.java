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

package org.apache.uniffle.common.util;

import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RssUtilsTest {

  @Test
  public void testGetPropertiesFromFile() {
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("rss-defaults.conf")).getFile();
    Map<String, String> properties = RssUtils.getPropertiesFromFile(filePath);
    assertEquals("12121", properties.get("rss.coordinator.port"));
    assertEquals("155", properties.get("rss.server.heartbeat.interval"));
    assertEquals("true", properties.get("rss.x.y.z"));
    assertEquals("-XX:+PrintGCDetails-Dkey=value-Dnumbers=\"one two three\"",
        properties.get("rss.a.b.c.extraJavaOptions"));
  }

  @Test
  public void testGetHostIp() {
    try {
      String realIp = RssUtils.getHostIp();
      InetAddress ia = InetAddress.getByName(realIp);
      assertTrue(ia instanceof Inet4Address);
      assertFalse(ia.isLinkLocalAddress() || ia.isAnyLocalAddress() || ia.isLoopbackAddress());
      assertTrue(NetworkInterface.getByInetAddress(ia) != null);
      assertTrue(ia.isReachable(5000));
      setEnv("RSS_IP", "8.8.8.8");
      assertEquals("8.8.8.8", RssUtils.getHostIp());
      setEnv("RSS_IP", "xxxx");
      boolean isException = false;
      try {
        RssUtils.getHostIp();
      } catch (Exception e) {
        isException = true;
      }
      setEnv("RSS_IP", realIp);
      RssUtils.getHostIp();
      assertTrue(isException);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSerializeBitmap() throws Exception {
    Roaring64NavigableMap bitmap1 = Roaring64NavigableMap.bitmapOf(1, 2, 100, 10000);
    byte[] bytes = RssUtils.serializeBitMap(bitmap1);
    Roaring64NavigableMap bitmap2 = RssUtils.deserializeBitMap(bytes);
    assertEquals(bitmap1, bitmap2);
    assertEquals(Roaring64NavigableMap.bitmapOf(), RssUtils.deserializeBitMap(new byte[]{}));
  }

  @Test
  public void testCloneBitmap() {
    Roaring64NavigableMap bitmap1 = Roaring64NavigableMap.bitmapOf(1, 2, 100, 10000);
    Roaring64NavigableMap bitmap2 = RssUtils.cloneBitMap(bitmap1);
    assertNotSame(bitmap1, bitmap2);
    assertEquals(bitmap1, bitmap2);
  }

  @Test
  public void getMetricNameForHostNameTest() {
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a.b.c"));
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a-b-c"));
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a.b-c"));
  }

  @Test
  public void testLoadExtentions() {
    List<String> exts = Collections.singletonList("Dummy");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, 1);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().startsWith("java.lang.ClassNotFoundException: Dummy"));
    }
    exts = Collections.singletonList("org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummyFailNotSub");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, 1);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("RssUtilTestDummyFailNotSub is not subclass of "
          + "org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummy"));
    }
    exts = Collections.singletonList("org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummyNoConstructor");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, "Test");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("RssUtilTestDummyNoConstructor.<init>()"));
    }
    exts = Collections.singletonList("org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummySuccess");
    String testStr = String.valueOf(new Random().nextInt());
    List<RssUtilTestDummy> extsObjs = RssUtils.loadExtensions(RssUtilTestDummy.class, exts, testStr);
    assertEquals(1, extsObjs.size());
    assertEquals(testStr, extsObjs.get(0).get());
  }

  @Test
  public void testShuffleBitmapToPartitionBitmap() {
    Roaring64NavigableMap partition1Bitmap = Roaring64NavigableMap.bitmapOf(
        getBlockId(0, 0, 0),
        getBlockId(0, 0, 1),
        getBlockId(0, 1, 0),
        getBlockId(0, 1, 1));
    Roaring64NavigableMap partition2Bitmap = Roaring64NavigableMap.bitmapOf(
        getBlockId(1, 0, 0),
        getBlockId(1, 0, 1),
        getBlockId(1, 1, 0),
        getBlockId(1, 1, 1));
    Roaring64NavigableMap shuffleBitmap = Roaring64NavigableMap.bitmapOf();
    shuffleBitmap.or(partition1Bitmap);
    shuffleBitmap.or(partition2Bitmap);
    assertEquals(8, shuffleBitmap.getLongCardinality());
    Map<Integer, Roaring64NavigableMap> toPartitionBitmap =
        RssUtils.generatePartitionToBitmap(shuffleBitmap, 0, 2);
    assertEquals(2, toPartitionBitmap.size());
    assertEquals(partition1Bitmap, toPartitionBitmap.get(0));
    assertEquals(partition2Bitmap, toPartitionBitmap.get(1));
  }

  @Test
  public void testGenerateServerToPartitions() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    ShuffleServerInfo server1 = new ShuffleServerInfo("server1", "0.0.0.1", 100);
    ShuffleServerInfo server2 = new ShuffleServerInfo("server2", "0.0.0.2", 200);
    ShuffleServerInfo server3 = new ShuffleServerInfo("server3", "0.0.0.3", 300);
    ShuffleServerInfo server4 = new ShuffleServerInfo("server4", "0.0.0.4", 400);
    partitionToServers.put(1, Lists.newArrayList(server1, server2));
    partitionToServers.put(2, Lists.newArrayList(server3, server4));
    partitionToServers.put(3, Lists.newArrayList(server1, server2));
    partitionToServers.put(4, Lists.newArrayList(server3, server4));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions = RssUtils.generateServerToPartitions(partitionToServers);
    assertEquals(4, serverToPartitions.size());
    assertEquals(serverToPartitions.get(server1), Sets.newHashSet(1, 3));
    assertEquals(serverToPartitions.get(server2), Sets.newHashSet(1, 3));
    assertEquals(serverToPartitions.get(server3), Sets.newHashSet(2, 4));
    assertEquals(serverToPartitions.get(server4), Sets.newHashSet(2, 4));
  }

  @Test
  public void testMergeRangeSegments() {
    List<Long> endPoints = Lists.newArrayList(1L, 2L, 5L, 6L);
    List<Long> results = RssUtils.mergeRangeSegments(endPoints, 1);
    ArrayList<Long> expectResults = Lists.newArrayList(1L, 6L);
    assertIterableEquals(results, expectResults);

    endPoints = Lists.newArrayList(1L, 2L, 5L, 6L, 8L, 10L);
    results = RssUtils.mergeRangeSegments(endPoints, 2);
    expectResults = Lists.newArrayList(1L, 2L, 5L, 10L);
    assertIterableEquals(results, expectResults);

    endPoints = Lists.newArrayList(1L, 2L, 5L, 6L, 10L, 12L);
    results = RssUtils.mergeRangeSegments(endPoints, 2);
    expectResults = Lists.newArrayList(1L, 6L, 10L, 12L);
    assertIterableEquals(results, expectResults);

    endPoints = Lists.newArrayList(1L, 1L, 3L, 3L, 5L, 5L);
    results = RssUtils.mergeRangeSegments(endPoints, 2);
    expectResults = Lists.newArrayList(1L, 3L, 5L, 5L);
    assertIterableEquals(results, expectResults);
  }

  @Test
  public void testGenerateRangeSegments() {
    Random random = new Random();
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf();
    for (int i = 0; i < 1000; i++) {
      if (random.nextInt(10) < 3) {
        continue;
      }
      bitmap.add(i);
    }
    int maxSegments = 3;
    List<Long> segments = RssUtils.generateRangeSegments(bitmap, 3);
    assertEquals(maxSegments * 2, segments.size());

    List<Long> endPoints = Lists.newArrayList(1L, 2L, 5L, 6L);
    bitmap = Roaring64NavigableMap.bitmapOf();
    for (Long endPoint : endPoints) {
      bitmap.add(endPoint);
    }
    List<Long> results = RssUtils.generateRangeSegments(bitmap, 100);
    List<Long> expectResults = Lists.newArrayList(1L, 2L, 5L, 6L);
    assertIterableEquals(results, expectResults);

    bitmap = Roaring64NavigableMap.bitmapOf();
    endPoints = Lists.newArrayList(1L, 2L, 5L, 6L, 8L, 9L, 10L);
    for (Long endPoint : endPoints) {
      bitmap.add(endPoint);
    }
    results = RssUtils.generateRangeSegments(bitmap, 100);
    expectResults = Lists.newArrayList(1L, 2L, 5L, 6L, 8L, 10L);
    assertIterableEquals(results, expectResults);

    bitmap = Roaring64NavigableMap.bitmapOf();
    endPoints = Lists.newArrayList(1L, 2L, 5L, 6L, 10L, 11L, 12L);
    for (Long endPoint : endPoints) {
      bitmap.add(endPoint);
    }
    results = RssUtils.generateRangeSegments(bitmap, 100);
    expectResults = Lists.newArrayList(1L, 2L, 5L, 6L, 10L, 12L);
    assertIterableEquals(results, expectResults);

    bitmap = Roaring64NavigableMap.bitmapOf();
    endPoints = Lists.newArrayList(1L, 1L, 3L, 3L, 5L, 5L);
    for (Long endPoint : endPoints) {
      bitmap.add(endPoint);
    }
    results = RssUtils.generateRangeSegments(bitmap, 100);
    expectResults = Lists.newArrayList(1L, 1L, 3L, 3L, 5L, 5L);
    assertIterableEquals(results, expectResults);
  }

  @Test
  public void testCheckIfBlockInRange() {
    List<Long> rangeSegments = Lists.newArrayList(1L, 2L, 5L, 6L, 10L, 12L);
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf();
    for (Long element : rangeSegments) {
      bitmap.add(element);
    }
    assertFalse(RssUtils.checkIfBlockInRange(rangeSegments, 13L));
    assertTrue(RssUtils.checkIfBlockInRange(rangeSegments, 10L));
    assertFalse(RssUtils.checkIfBlockInRange(rangeSegments, 9L));
    assertTrue(RssUtils.checkIfBlockInRange(rangeSegments, 5L));
    assertFalse(RssUtils.checkIfBlockInRange(rangeSegments, 4L));
    assertFalse(RssUtils.checkIfBlockInRange(rangeSegments, 3L));
    assertTrue(RssUtils.checkIfBlockInRange(rangeSegments, 2L));
    assertTrue(RssUtils.checkIfBlockInRange(rangeSegments, 1L));
    assertFalse(RssUtils.checkIfBlockInRange(rangeSegments, 0L));
  }

  // Copy from ClientUtils
  private Long getBlockId(long partitionId, long taskAttemptId, long atomicInt) {
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskAttemptId;
  }

  interface RssUtilTestDummy {
    String get();
  }

  public static class RssUtilTestDummyFailNotSub {
    public RssUtilTestDummyFailNotSub() {
    }
  }

  public static class RssUtilTestDummyNoConstructor implements RssUtilTestDummy {
    public RssUtilTestDummyNoConstructor(int a) {
    }

    public String get() {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  public static class RssUtilTestDummySuccess implements RssUtilTestDummy {
    private final String s;

    public RssUtilTestDummySuccess(String s) {
      this.s = s;
    }

    public String get() {
      return s;
    }
  }


}
