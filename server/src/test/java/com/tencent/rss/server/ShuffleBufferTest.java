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

package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Random;
import org.junit.Test;

public class ShuffleBufferTest {

  static {
    ShuffleServerMetrics.register();
  }

  @Test
  public void appendTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(26));
    assertEquals(100, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(1));
    assertEquals(133, shuffleBuffer.getSize());
    assertTrue(shuffleBuffer.isFull());
  }

  @Test
  public void toFlushEventTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    ShuffleDataFlushEvent event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertNull(event);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertEquals(42, event.getSize());
    assertEquals(0, shuffleBuffer.getSize());
    assertEquals(0, shuffleBuffer.getBlocks().size());
  }

  private ShufflePartitionedData createData(int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, len, 1, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(1, new ShufflePartitionedBlock[]{block});
    return data;
  }

}
