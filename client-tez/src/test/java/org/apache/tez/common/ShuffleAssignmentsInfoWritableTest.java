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

package org.apache.tez.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleAssignmentsInfoWritableTest {
  @Test
  public void testSerDe() throws IOException {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(0, new ArrayList<>());
    partitionToServers.put(1, new ArrayList<>());
    partitionToServers.put(2, new ArrayList<>());
    partitionToServers.put(3, new ArrayList<>());
    partitionToServers.put(4, new ArrayList<>());

    ShuffleServerInfo work1 = new ShuffleServerInfo("host1", 9999);
    ShuffleServerInfo work2 = new ShuffleServerInfo("host1", 9999);
    ShuffleServerInfo work3 = new ShuffleServerInfo("host1", 9999);
    ShuffleServerInfo work4 = new ShuffleServerInfo("host1", 9999);

    partitionToServers.get(0).addAll(Arrays.asList(work1, work2, work3, work4));
    partitionToServers.get(1).addAll(Arrays.asList(work1, work2, work3, work4));
    partitionToServers.get(2).addAll(Arrays.asList(work1, work3));
    partitionToServers.get(3).addAll(Arrays.asList(work3, work4));
    partitionToServers.get(4).addAll(Arrays.asList(work2, work4));

    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
    PartitionRange range0 = new PartitionRange(0, 0);
    PartitionRange range1 = new PartitionRange(1, 1);
    PartitionRange range2 = new PartitionRange(2, 2);
    PartitionRange range3 = new PartitionRange(3, 3);
    PartitionRange range4 = new PartitionRange(4, 4);

    serverToPartitionRanges.put(work1, Arrays.asList(range0, range1, range2));
    serverToPartitionRanges.put(work2, Arrays.asList(range0, range1, range4));
    serverToPartitionRanges.put(work3, Arrays.asList(range0, range1, range2, range3));
    serverToPartitionRanges.put(work4, Arrays.asList(range0, range1, range3, range4));

    ShuffleAssignmentsInfo info =
        new ShuffleAssignmentsInfo(partitionToServers, serverToPartitionRanges);
    ShuffleAssignmentsInfoWritable infoWritable = new ShuffleAssignmentsInfoWritable(info);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bos);
    infoWritable.write(out);

    ShuffleAssignmentsInfoWritable deSerInfoWritable = new ShuffleAssignmentsInfoWritable();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInput in = new DataInputStream(bis);
    deSerInfoWritable.readFields(in);

    {
      Map<Integer, List<ShuffleServerInfo>> base =
          infoWritable.getShuffleAssignmentsInfo().getPartitionToServers();
      Map<Integer, List<ShuffleServerInfo>> deSer =
          deSerInfoWritable.getShuffleAssignmentsInfo().getPartitionToServers();

      assertEquals(base.size(), deSer.size());
      for (Integer partitionId : base.keySet()) {
        assertEquals(base.get(partitionId), deSer.get(partitionId));
      }
    }
    {
      Map<ShuffleServerInfo, List<PartitionRange>> base =
          infoWritable.getShuffleAssignmentsInfo().getServerToPartitionRanges();
      Map<ShuffleServerInfo, List<PartitionRange>> deSer =
          deSerInfoWritable.getShuffleAssignmentsInfo().getServerToPartitionRanges();

      assertEquals(base.size(), deSer.size());
      for (ShuffleServerInfo serverInfo : base.keySet()) {
        assertEquals(base.get(serverInfo), deSer.get(serverInfo));
      }
    }
  }
}
