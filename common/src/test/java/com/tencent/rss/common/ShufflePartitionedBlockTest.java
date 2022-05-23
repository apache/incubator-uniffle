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

package com.tencent.rss.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.junit.jupiter.api.Test;

public class ShufflePartitionedBlockTest {

  @Test
  public void shufflePartitionedBlockTest() {
    byte[] buf = new byte[3];
    new Random().nextBytes(buf);

    ShufflePartitionedBlock b1 = new ShufflePartitionedBlock(1, 1, 2, 3, 1, buf);
    assertEquals(1, b1.getLength());
    assertEquals(2, b1.getCrc());
    assertEquals(3, b1.getBlockId());

    ShufflePartitionedBlock b3 = new ShufflePartitionedBlock(1, 1, 2, 3, 3, buf);
    assertArrayEquals(buf, b3.getData());
  }
}
