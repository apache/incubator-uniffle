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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BlockIdTest {
  BlockIdLayout layout1 = BlockIdLayout.DEFAULT;
  BlockId blockId1 = layout1.asBlockId(1, 2, 3);
  BlockId blockId2 = layout1.asBlockId(15, 30, 63);

  BlockIdLayout layout2 = BlockIdLayout.from(31, 16, 16);
  BlockId blockId3 = layout2.asBlockId(1, 2, 3);
  BlockId blockId4 = layout2.asBlockId(15, 30, 63);

  BlockId opaqueBlockId1 = new OpaqueBlockId(blockId1.getBlockId());
  BlockId opaqueBlockId2 = new OpaqueBlockId(blockId2.getBlockId());
  BlockId opaqueBlockId3 = new OpaqueBlockId(blockId3.getBlockId());
  BlockId opaqueBlockId4 = new OpaqueBlockId(blockId4.getBlockId());

  @Test
  public void testGetters() {
    assertEquals(35184376283139L, blockId1.getBlockId());
    assertEquals(527765644247103L, blockId2.getBlockId());
    assertEquals(4295098371L, blockId3.getBlockId());
    assertEquals(64426475583L, blockId4.getBlockId());
    assertEquals(35184376283139L, opaqueBlockId1.getBlockId());
    assertEquals(527765644247103L, opaqueBlockId2.getBlockId());
    assertEquals(4295098371L, opaqueBlockId3.getBlockId());
    assertEquals(64426475583L, opaqueBlockId4.getBlockId());

    assertEquals(1, blockId1.getSequenceNo());
    assertEquals(15, blockId2.getSequenceNo());
    assertEquals(1, blockId3.getSequenceNo());
    assertEquals(15, blockId4.getSequenceNo());
    assertThrows(UnsupportedOperationException.class, () -> opaqueBlockId1.getSequenceNo());

    assertEquals(2, blockId1.getPartitionId());
    assertEquals(30, blockId2.getPartitionId());
    assertEquals(2, blockId3.getPartitionId());
    assertEquals(30, blockId4.getPartitionId());
    assertThrows(UnsupportedOperationException.class, () -> opaqueBlockId1.getPartitionId());

    assertEquals(3, blockId1.getTaskAttemptId());
    assertEquals(63, blockId2.getTaskAttemptId());
    assertEquals(3, blockId3.getTaskAttemptId());
    assertEquals(63, blockId4.getTaskAttemptId());
    assertThrows(UnsupportedOperationException.class, () -> opaqueBlockId1.getTaskAttemptId());
  }

  @Test
  public void testToString() {
    assertEquals("200000400003 (seq: 1, part: 2, task: 3)", blockId1.toString());
    assertEquals("1e00003c0003f (seq: 15, part: 30, task: 63)", blockId2.toString());
    assertEquals("100020003 (seq: 1, part: 2, task: 3)", blockId3.toString());
    assertEquals("f001e003f (seq: 15, part: 30, task: 63)", blockId4.toString());

    assertEquals("200000400003", opaqueBlockId1.toString());
    assertEquals("1e00003c0003f", opaqueBlockId2.toString());
    assertEquals("100020003", opaqueBlockId3.toString());
    assertEquals("f001e003f", opaqueBlockId4.toString());
  }

  @Test
  public void testEquals() {
    assertEquals(blockId1, blockId1);
    assertSame(blockId1, blockId1);

    assertNotEquals(blockId1, blockId2);
    assertNotEquals(blockId1, blockId3);
    assertNotEquals(blockId1, blockId4);
    assertNotEquals(blockId2, blockId4);

    assertEquals(blockId1, opaqueBlockId1);
    assertEquals(blockId2, opaqueBlockId2);
    assertEquals(blockId3, opaqueBlockId3);
    assertEquals(blockId4, opaqueBlockId4);
    assertNotSame(blockId1, opaqueBlockId1);
    assertNotSame(blockId2, opaqueBlockId2);
    assertNotSame(blockId3, opaqueBlockId3);
    assertNotSame(blockId4, opaqueBlockId4);
    assertNotEquals(blockId1, opaqueBlockId2);
    assertNotEquals(blockId1, opaqueBlockId3);
    assertNotEquals(blockId1, opaqueBlockId4);

    assertEquals(opaqueBlockId1, opaqueBlockId1);
    assertEquals(opaqueBlockId1, new OpaqueBlockId(opaqueBlockId1.getBlockId()));

    BlockId blockId4 = layout1.asBlockId(1, 2, 3);
    assertEquals(blockId1, blockId4);
    assertNotSame(blockId1, blockId4);
  }
}
