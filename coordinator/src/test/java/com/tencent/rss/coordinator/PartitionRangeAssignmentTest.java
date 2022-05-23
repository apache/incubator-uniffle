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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Sets;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.proto.RssProtos;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class PartitionRangeAssignmentTest {

  @Test
  public void test() {
    SortedMap sortedMap = new TreeMap();
    for (int i = 0; i < 9; i = i + 3) {
      PartitionRange range = new PartitionRange(i, i + 2);
      List<ServerNode> nodes = Collections.singletonList(new ServerNode(
          String.valueOf(i), "", i / 3, 0, 0, 0, 0, Sets.newHashSet("test"), true));
      sortedMap.put(range, nodes);
    }

    PartitionRangeAssignment partitionRangeAssignment = new PartitionRangeAssignment(sortedMap);
    List<RssProtos.PartitionRangeAssignment> res = partitionRangeAssignment.convertToGrpcProto();
    assertEquals(3, res.size());

    for (int i = 0; i < 3; ++i) {
      RssProtos.PartitionRangeAssignment pra = res.get(i);
      assertEquals(1, pra.getServerCount());
      assertEquals(i, pra.getServer(0).getPort());
      assertEquals(3 * i, pra.getStartPartition());
      assertEquals(3 * i + 2, pra.getEndPartition());
    }

    partitionRangeAssignment = new PartitionRangeAssignment(null);
    res = partitionRangeAssignment.convertToGrpcProto();
    assertTrue(res.isEmpty());
  }
}
