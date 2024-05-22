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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BlockIdLayoutTest {
  @Test
  public void testFromLengths() {
    BlockIdLayout layout = BlockIdLayout.from(20, 21, 22);

    assertEquals(20, layout.sequenceNoBits);
    assertEquals(21, layout.partitionIdBits);
    assertEquals(22, layout.taskAttemptIdBits);

    assertEquals(43, layout.sequenceNoOffset);
    assertEquals(22, layout.partitionIdOffset);
    assertEquals(0, layout.taskAttemptIdOffset);

    assertEquals(1048575, layout.maxSequenceNo);
    assertEquals(2097151, layout.maxPartitionId);
    assertEquals(4194303, layout.maxTaskAttemptId);

    assertEquals(2097152, layout.maxNumPartitions);

    assertEquals(1048575L << (21 + 22), layout.sequenceNoMask);
    assertEquals(2097151L << 22, layout.partitionIdMask);
    assertEquals(4194303L, layout.taskAttemptIdMask);

    assertEquals("blockIdLayout[seq: 20 bits, part: 21 bits, task: 22 bits]", layout.toString());
  }

  @Test
  public void testFromLengthsErrors() {
    final Throwable e1 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(21, 21, 20));
    assertEquals(
        "Don't support given lengths, sum must be exactly 63: 21 + 21 + 20 = 62", e1.getMessage());
    final Throwable e2 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(21, 21, 22));
    assertEquals(
        "Don't support given lengths, sum must be exactly 63: 21 + 21 + 22 = 64", e2.getMessage());

    final Throwable e3 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(32, 16, 16));
    assertEquals(
        "Don't support given lengths, individual lengths must be less that 32: "
            + "given sequenceNoBits=32, partitionIdBits=16, taskAttemptIdBits=16",
        e3.getMessage());
    final Throwable e4 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(16, 32, 16));
    assertEquals(
        "Don't support given lengths, individual lengths must be less that 32: "
            + "given sequenceNoBits=16, partitionIdBits=32, taskAttemptIdBits=16",
        e4.getMessage());
    final Throwable e5 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(16, 16, 32));
    assertEquals(
        "Don't support given lengths, individual lengths must be less that 32: "
            + "given sequenceNoBits=16, partitionIdBits=16, taskAttemptIdBits=32",
        e5.getMessage());

    final Throwable e6 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(22, 21, 21));
    assertEquals(
        "Don't support given lengths, sum must be exactly 63: 22 + 21 + 21 = 64", e6.getMessage());
    final Throwable e7 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(21, 22, 21));
    assertEquals(
        "Don't support given lengths, sum must be exactly 63: 21 + 22 + 21 = 64", e7.getMessage());
    final Throwable e8 =
        assertThrows(IllegalArgumentException.class, () -> BlockIdLayout.from(21, 21, 22));
    assertEquals(
        "Don't support given lengths, sum must be exactly 63: 21 + 21 + 22 = 64", e8.getMessage());
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT),
        Arguments.of(BlockIdLayout.from(21, 21, 21)),
        Arguments.of(BlockIdLayout.from(12, 24, 27)),
        Arguments.of(BlockIdLayout.from(1, 31, 31)),
        Arguments.of(BlockIdLayout.from(31, 1, 31)),
        Arguments.of(BlockIdLayout.from(31, 31, 1)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testLayoutGetBlockId(BlockIdLayout layout) {
    // max value of blockId
    assertEquals(
        (long) layout.maxSequenceNo << layout.sequenceNoOffset
            | (long) layout.maxPartitionId << layout.partitionIdOffset
            | (long) layout.maxTaskAttemptId << layout.taskAttemptIdOffset,
        layout.getBlockId(layout.maxSequenceNo, layout.maxPartitionId, layout.maxTaskAttemptId));

    // min value of blockId
    assertEquals(0L, layout.getBlockId(0, 0, 0));

    // just a random test
    if (layout.sequenceNoBits > 7 && layout.partitionIdBits > 6 && layout.taskAttemptIdBits > 7) {
      long blockId = layout.getBlockId(123, 45, 67);
      assertEquals(
          123L << layout.sequenceNoOffset
              | 45L << layout.partitionIdOffset
              | 67L << layout.taskAttemptIdOffset,
          blockId);

      assertEquals(123, layout.getSequenceNo(blockId));
      assertEquals(45, layout.getPartitionId(blockId));
      assertEquals(67, layout.getTaskAttemptId(blockId));

      assertEquals(blockId, layout.asBlockId(blockId).blockId);
      assertEquals(123, layout.asBlockId(blockId).sequenceNo);
      assertEquals(45, layout.asBlockId(blockId).partitionId);
      assertEquals(67, layout.asBlockId(blockId).taskAttemptId);
      assertEquals(layout, layout.asBlockId(blockId).layout);
    }

    final Throwable e1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> layout.getBlockId(layout.maxSequenceNo + 1, 0, 0));
    assertEquals(
        "Don't support sequenceNo["
            + (layout.maxSequenceNo + 1)
            + "], "
            + "the max value should be "
            + layout.maxSequenceNo,
        e1.getMessage());

    final Throwable e2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> layout.getBlockId(0, layout.maxPartitionId + 1, 0));
    assertEquals(
        "Don't support partitionId["
            + (layout.maxPartitionId + 1)
            + "], "
            + "the max value should be "
            + layout.maxPartitionId,
        e2.getMessage());

    final Throwable e3 =
        assertThrows(
            IllegalArgumentException.class,
            () -> layout.getBlockId(0, 0, layout.maxTaskAttemptId + 1));
    assertEquals(
        "Don't support taskAttemptId["
            + (layout.maxTaskAttemptId + 1)
            + "], "
            + "the max value should be "
            + layout.maxTaskAttemptId,
        e3.getMessage());
  }

  @Test
  public void testEquals() {
    BlockIdLayout layout1 = BlockIdLayout.from(20, 21, 22);
    BlockIdLayout layout2 = BlockIdLayout.from(20, 21, 22);
    BlockIdLayout layout3 = BlockIdLayout.from(18, 22, 23);

    assertEquals(layout1, layout1);
    assertEquals(layout1, layout2);
    assertNotEquals(layout1, layout3);

    BlockIdLayout layout4 = BlockIdLayout.from(18, 24, 21);
    assertNotEquals(layout1, layout4);
    assertEquals(layout4, BlockIdLayout.DEFAULT);
  }
}
