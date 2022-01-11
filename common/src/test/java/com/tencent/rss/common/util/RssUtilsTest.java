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

package com.tencent.rss.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

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
      String address = InetAddress.getLocalHost().getHostAddress();
      String realIp = RssUtils.getHostIp();
      assertNotEquals("127.0.0.1", realIp);
      if (!address.equals("127.0.0.1")) {
        assertEquals(address, realIp);
      }
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
}
