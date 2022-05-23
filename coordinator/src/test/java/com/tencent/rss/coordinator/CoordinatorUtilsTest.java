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

import com.tencent.rss.common.PartitionRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CoordinatorUtilsTest {

  @Test
  public void testNextId() {
    assertEquals(1, CoordinatorUtils.nextIdx(0, 3));
    assertEquals(2, CoordinatorUtils.nextIdx(1, 3));
    assertEquals(0, CoordinatorUtils.nextIdx(2, 3));
  }

  @Test
  public void testGenerateRanges() {
    List<PartitionRange> ranges = CoordinatorUtils.generateRanges(16, 5);
    assertEquals(new PartitionRange(0, 4), ranges.get(0));
    assertEquals(new PartitionRange(5, 9), ranges.get(1));
    assertEquals(new PartitionRange(10, 14), ranges.get(2));
    assertEquals(new PartitionRange(15, 19), ranges.get(3));
  }
}
