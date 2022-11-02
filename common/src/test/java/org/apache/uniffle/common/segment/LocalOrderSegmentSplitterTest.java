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

package org.apache.uniffle.common.segment;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalOrderSegmentSplitterTest {

  @Test
  public void testSplit() {
    LocalOrderSegmentSplitter splitter = new LocalOrderSegmentSplitter(1, 2, 1000);
    assertTrue(splitter.split(new ShuffleIndexResult()).isEmpty());

    splitter = new LocalOrderSegmentSplitter(1, 2, 32);

    /**
     * (length, taskId)
     * case1: (32, 1) (16, 1) (10, 2) (16, 1) (6, 1)
     *
     *        (10, 2) will be dropped
     */
    byte[] data = generateData(
        Pair.of(32, 1),
        Pair.of(16, 1),
        Pair.of(10, 2),
        Pair.of(16, 1),
        Pair.of(6, 1)
    );
    List<ShuffleDataSegment> dataSegments = splitter.split(new ShuffleIndexResult(data, -1));
    assertEquals(3, dataSegments.size());

    assertEquals(0, dataSegments.get(0).getOffset());
    assertEquals(32, dataSegments.get(0).getLength());

    assertEquals(32, dataSegments.get(1).getOffset());
    assertEquals(16, dataSegments.get(1).getLength());

    assertEquals(58, dataSegments.get(2).getOffset());
    assertEquals(22, dataSegments.get(2).getLength());

    /**
     * case2: (32, 2) (16, 1) (10, 1) (16, 2) (6, 1)
     *
     *        (32, 2) (16, 2) will be dropped
     */
    data = generateData(
        Pair.of(32, 2),
        Pair.of(16, 1),
        Pair.of(10, 1),
        Pair.of(16, 2),
        Pair.of(6, 1)
    );
    dataSegments = splitter.split(new ShuffleIndexResult(data, -1));
    assertEquals(2, dataSegments.size());

    assertEquals(32, dataSegments.get(0).getOffset());
    assertEquals(26, dataSegments.get(0).getLength());

    assertEquals(74, dataSegments.get(1).getOffset());
    assertEquals(6, dataSegments.get(1).getLength());

    /**
     * case3: (32, 5) (16, 1) (10, 3) (16, 4) (6, 1)
     *
     *        (32, 5) will be dropped
     */
    splitter = new LocalOrderSegmentSplitter(1, 5, 32);
    data = generateData(
        Pair.of(32, 5),
        Pair.of(16, 1),
        Pair.of(10, 3),
        Pair.of(16, 4),
        Pair.of(6, 1)
    );
    dataSegments = splitter.split(new ShuffleIndexResult(data, -1));
    assertEquals(2, dataSegments.size());

    assertEquals(32, dataSegments.get(0).getOffset());
    assertEquals(42, dataSegments.get(0).getLength());

    assertEquals(74, dataSegments.get(1).getOffset());
    assertEquals(6, dataSegments.get(1).getLength());
  }

  public static byte[] generateData(Pair<Integer, Integer>... configEntries) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(configEntries.length * 40);
    int total = 0;
    for (Pair<Integer, Integer> entry : configEntries) {
      byteBuffer.putLong(total);
      byteBuffer.putInt(entry.getLeft());
      byteBuffer.putInt(1);
      byteBuffer.putLong(1);
      byteBuffer.putLong(1);
      byteBuffer.putLong(entry.getRight());

      total += entry.getLeft();
    }
    return byteBuffer.array();
  }
}
