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

public class BlockIdTest {
  @Test
  public void toStringTest() {
    BlockIdLayout layout1 = BlockIdLayout.DEFAULT;
    BlockId blockId1 = layout1.asBlockId(1, 2, 3);
    assertEquals("blockId[200000400003 (seq: 1, part: 2, task: 3)]", blockId1.toString());
    BlockId blockId2 = layout1.asBlockId(15, 30, 63);
    assertEquals("blockId[1e00003c0003f (seq: 15, part: 30, task: 63)]", blockId2.toString());

    BlockIdLayout layout2 = BlockIdLayout.from(31, 16, 16);
    BlockId blockId3 = layout2.asBlockId(1, 2, 3);
    assertEquals("blockId[100020003 (seq: 1, part: 2, task: 3)]", blockId3.toString());
    BlockId blockId4 = layout2.asBlockId(15, 30, 63);
    assertEquals("blockId[f001e003f (seq: 15, part: 30, task: 63)]", blockId4.toString());
  }
}
