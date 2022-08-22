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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  public void testShuffleIndexSegment() {
    ShuffleIndexResult shuffleIndexResult = new ShuffleIndexResult();
    List<ShuffleDataSegment> shuffleDataSegments =
        RssUtils.transIndexDataToSegments(shuffleIndexResult, 1000);
    assertTrue(shuffleDataSegments.isEmpty());

    int readBufferSize = 32;
    int totalLength = 0;
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    int[] dataSegmentLength = new int[]{32, 16, 10, 32, 6};

    for (int i = 0; i < dataSegmentLength.length; ++i) {
      long offset = totalLength;
      int length = dataSegmentLength[i];
      bufferSegments.add(new BufferSegment(i, offset, length, i, i, i));
      totalLength += length;
    }

    // those 5 segment's data length are [32, 16, 10, 32, 6] so the index should be
    // split into 3 ShuffleDataSegment, which are [32, 16 + 10 + 32, 6]
    int expectedTotalSegmentNum = 3;
    ByteBuffer byteBuffer = ByteBuffer.allocate(5 * 40);

    for (BufferSegment bufferSegment : bufferSegments) {
      byteBuffer.putLong(bufferSegment.getOffset());
      byteBuffer.putInt(bufferSegment.getLength());
      byteBuffer.putInt(bufferSegment.getUncompressLength());
      byteBuffer.putLong(bufferSegment.getCrc());
      byteBuffer.putLong(bufferSegment.getBlockId());
      byteBuffer.putLong(bufferSegment.getTaskAttemptId());
    }

    byte[] data = byteBuffer.array();
    shuffleDataSegments = RssUtils.transIndexDataToSegments(new ShuffleIndexResult(data), readBufferSize);
    assertEquals(expectedTotalSegmentNum, shuffleDataSegments.size());

    assertEquals(0, shuffleDataSegments.get(0).getOffset());
    assertEquals(32, shuffleDataSegments.get(0).getLength());
    assertEquals(1, shuffleDataSegments.get(0).getBufferSegments().size());

    assertEquals(32, shuffleDataSegments.get(1).getOffset());
    assertEquals(58, shuffleDataSegments.get(1).getLength());
    assertEquals(3,shuffleDataSegments.get(1).getBufferSegments().size());

    assertEquals(90, shuffleDataSegments.get(2).getOffset());
    assertEquals(6, shuffleDataSegments.get(2).getLength());
    assertEquals(1, shuffleDataSegments.get(2).getBufferSegments().size());

    ByteBuffer incompleteByteBuffer = ByteBuffer.allocate(12);
    incompleteByteBuffer.putLong(1L);
    incompleteByteBuffer.putInt(2);
    data = incompleteByteBuffer.array();
    assertTrue(RssUtils.transIndexDataToSegments(new ShuffleIndexResult(data), readBufferSize).isEmpty());
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
