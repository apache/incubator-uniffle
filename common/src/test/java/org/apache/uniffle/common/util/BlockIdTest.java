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
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BlockIdTest {
  @Test
  public void test() {
    // max value of blockId
    assertEquals(854558029292503039L, BlockId.getBlockId(24287, 16777215, 1048575));
    // just a random test
    assertEquals(3518437418598500L, BlockId.getBlockId(100, 100, 100));
    // min value of blockId
    assertEquals(0L, BlockId.getBlockId(0, 0, 0));

    BlockId blockId = BlockId.fromIds(100, 100, 100);
    assertEquals("blockId[c80000c800064 (seq: 100, part: 100, task: 100)]", blockId.toString());

    final Throwable e1 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(262144, 0, 0));
    assertEquals("Can't support sequence[262144], the max value should be 262143", e1.getMessage());

    final Throwable e2 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(0, 16777216, 0));
    assertEquals(
        "Can't support partitionId[16777216], the max value should be 16777215", e2.getMessage());

    final Throwable e3 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(0, 0, 2097152));
    assertEquals(
        "Can't support taskAttemptId[2097152], the max value should be 2097151", e3.getMessage());

    final Throwable e4 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(-1, 0, 0));
    assertEquals("Can't support sequence[-1], the max value should be 262143", e4.getMessage());

    final Throwable e5 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(0, -1, 0));
    assertEquals(
        "Can't support partitionId[-1], the max value should be 16777215", e5.getMessage());

    final Throwable e6 =
        assertThrows(IllegalArgumentException.class, () -> BlockId.getBlockId(0, 0, -1));
    assertEquals(
        "Can't support taskAttemptId[-1], the max value should be 2097151", e6.getMessage());
  }
}
