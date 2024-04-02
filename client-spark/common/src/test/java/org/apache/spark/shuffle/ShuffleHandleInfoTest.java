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

package org.apache.spark.shuffle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleHandleInfoTest {

  private ShuffleServerInfo createFakeServerInfo(String host) {
    return new ShuffleServerInfo(host, 1);
  }

  @Test
  public void testReassignment() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    ShuffleHandleInfo handleInfo =
        new ShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // case1
    assertFalse(handleInfo.isExistingFaultyServer("a"));
    Set<Integer> partitions = new HashSet<>();
    partitions.add(1);
    ShuffleServerInfo newServer = createFakeServerInfo("d");
    handleInfo.createNewReassignmentForMultiPartitions(partitions, "a", createFakeServerInfo("d"));
    assertTrue(handleInfo.isExistingFaultyServer("a"));

    assertEquals(newServer, handleInfo.useExistingReassignmentForMultiPartitions(partitions, "a"));
  }
}
