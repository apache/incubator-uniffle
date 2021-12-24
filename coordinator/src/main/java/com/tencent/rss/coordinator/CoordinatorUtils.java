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

import java.util.ArrayList;
import java.util.List;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;

public class CoordinatorUtils {

  public static GetShuffleAssignmentsResponse toGetShuffleAssignmentsResponse(
      PartitionRangeAssignment pra) {
    List<RssProtos.PartitionRangeAssignment> praList = pra.convertToGrpcProto();

    return GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(praList)
        .build();
  }

  public static int nextIdx(int idx, int size) {
    ++idx;
    if (idx >= size) {
      idx = 0;
    }
    return idx;
  }

  public static List<PartitionRange> generateRanges(int totalPartitionNum, int partitionNumPerRange) {
    List<PartitionRange> ranges = new ArrayList<>();
    if (totalPartitionNum <= 0 || partitionNumPerRange <= 0) {
      return ranges;
    }

    for (int start = 0; start < totalPartitionNum; start += partitionNumPerRange) {
      int end = start + partitionNumPerRange - 1;
      PartitionRange range = new PartitionRange(start, end);
      ranges.add(range);
    }

    return ranges;
  }
}
