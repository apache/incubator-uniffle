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

package com.tencent.rss.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.tencent.rss.common.PartitionRange;
import org.junit.jupiter.api.Test;

public class PartitionRangeTest {

  @Test
  public void test() {
    PartitionRange range1 = new PartitionRange(0, 5);
    PartitionRange range2 = new PartitionRange(0, 5);
    assertFalse(range1 == range2);
    assertEquals(range1, range2);
    assertEquals(0, range1.getStart());
    assertEquals(5, range1.getEnd());
  }

}
