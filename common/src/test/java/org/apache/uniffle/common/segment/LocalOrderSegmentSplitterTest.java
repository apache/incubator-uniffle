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
import java.util.ArrayList;
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
    LocalOrderSegmentSplitter splitter = new LocalOrderSegmentSplitter(1, 1, 1000);
    assertTrue(splitter.split(new ShuffleIndexResult()).isEmpty());

    splitter = new LocalOrderSegmentSplitter(1, 1, 32);

    /**
     * case1: (32, 1) (16, 1) (10, 2) (16, 1) (6, 1)
     *
     *        (10, 2) will be dropped
     */
    List<MockedIndexBlock> mockedBlocks = generateBlocks(
        Pair.of(32, 1),
        Pair.of(16, 1),
        Pair.of(10, 2),
        Pair.of(16, 1),
        Pair.of(6, 1)
    );
    byte[] data = generateData(mockedBlocks);
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
        generateBlocks(
            Pair.of(32, 2),
            Pair.of(16, 1),
            Pair.of(10, 1),
            Pair.of(16, 2),
            Pair.of(6, 1)
        )
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
    splitter = new LocalOrderSegmentSplitter(1, 4, 32);
    data = generateData(
        generateBlocks(
            Pair.of(32, 5),
            Pair.of(16, 1),
            Pair.of(10, 3),
            Pair.of(16, 4),
            Pair.of(6, 1)
        )
    );
    dataSegments = splitter.split(new ShuffleIndexResult(data, -1));
    assertEquals(2, dataSegments.size());

    assertEquals(32, dataSegments.get(0).getOffset());
    assertEquals(42, dataSegments.get(0).getLength());

    assertEquals(74, dataSegments.get(1).getOffset());
    assertEquals(6, dataSegments.get(1).getLength());
  }

  private byte[] generateData(List<MockedIndexBlock> mockedBlocks) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(mockedBlocks.size() * 40);
    int total = 0;
    for (MockedIndexBlock block : mockedBlocks) {
      byteBuffer.putLong(total);
      byteBuffer.putInt(block.getLength());
      byteBuffer.putInt(1);
      byteBuffer.putLong(1);
      byteBuffer.putLong(1);
      byteBuffer.putLong(block.getTaskId());

      total += block.getLength();
    }
    return byteBuffer.array();
  }

  static class MockedIndexBlock {
    private int length;
    private int taskId;

    public int getLength() {
      return length;
    }

    public int getTaskId() {
      return taskId;
    }

    public static Builder builder() {
      return new Builder();
    }

    static class Builder {
      private MockedIndexBlock mockedIndexBlock;

      public Builder() {
        this.mockedIndexBlock = new MockedIndexBlock();
      }

      public Builder length(int length) {
        this.mockedIndexBlock.length = length;
        return this;
      }

      public Builder taskId(int taskId) {
        this.mockedIndexBlock.taskId = taskId;
        return this;
      }

      public MockedIndexBlock build() {
        return mockedIndexBlock;
      }
    }
  }

  private List<MockedIndexBlock> generateBlocks(Pair<Integer, Integer>... configEntries) {
    List<MockedIndexBlock> blocks = new ArrayList<>();
    for (Pair<Integer, Integer> entry : configEntries) {
      blocks.add(
          MockedIndexBlock.builder().length(entry.getLeft()).taskId(entry.getRight()).build()
      );
    }
    return blocks;
  }
}
