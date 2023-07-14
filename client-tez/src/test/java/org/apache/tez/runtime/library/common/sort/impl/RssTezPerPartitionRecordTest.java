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

package org.apache.tez.runtime.library.common.sort.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezPerPartitionRecordTest {

  @Test
  public void testNumPartitions() {
    int[] numRecordsPerPartition = {0, 10, 10, 20, 30};
    int numOutputs = 5;

    RssTezPerPartitionRecord rssTezPerPartitionRecord =
        new RssTezPerPartitionRecord(numOutputs, numRecordsPerPartition);

    assertTrue(numOutputs == rssTezPerPartitionRecord.size());
  }

  @Test
  public void testRssTezIndexHasData() {
    int[] numRecordsPerPartition = {0, 10, 10, 20, 30};
    int numOutputs = 5;

    RssTezPerPartitionRecord rssTezPerPartitionRecord =
        new RssTezPerPartitionRecord(numOutputs, numRecordsPerPartition);

    for (int i = 0; i < numRecordsPerPartition.length; i++) {
      if (0 == i) {
        assertFalse(rssTezPerPartitionRecord.getIndex(i).hasData());
      }
      if (0 != i) {
        assertTrue(rssTezPerPartitionRecord.getIndex(i).hasData());
      }
    }
  }
}
