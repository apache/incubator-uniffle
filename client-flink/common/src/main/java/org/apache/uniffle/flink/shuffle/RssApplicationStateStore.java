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

package org.apache.uniffle.flink.shuffle;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

public class RssApplicationStateStore {
  private final AtomicInteger taskShuffleId = new AtomicInteger(0);

  // taskShuffleId <-> taskRssShuffleId
  BiMap<String, Integer> taskShuffleTable = HashBiMap.create();

  public Integer getRssTaskShuffleId(String shuffleId) {
    if (taskShuffleTable.containsKey(shuffleId)) {
      return taskShuffleTable.get(shuffleId);
    }
    int rssShuffleId = taskShuffleId.getAndIncrement();
    taskShuffleTable.put(shuffleId, rssShuffleId);
    return rssShuffleId;
  }

  public int getUnknowRssTaskShuffleId(String shuffleId) {
    if (taskShuffleTable.containsKey(shuffleId)) {
      return taskShuffleTable.get(shuffleId);
    }
    return -1;
  }

  // taskShuffleId -> partitionId -> rssPartitionId
  private final Table<String, String, AtomicInteger> taskPartitionsTable = HashBasedTable.create();

  public int getRssPartitionId(String taskShuffleId, String partitionId) {
    if (taskPartitionsTable.contains(taskShuffleId, partitionId)) {
      AtomicInteger atomicInteger = taskPartitionsTable.get(taskShuffleId, partitionId);
      assert atomicInteger != null;
      return atomicInteger.get();
    } else {
      AtomicInteger atomicInteger = new AtomicInteger(0);
      int rssPartitionId = atomicInteger.getAndIncrement();
      taskPartitionsTable.put(taskShuffleId, partitionId, atomicInteger);
      return rssPartitionId;
    }
  }

  // partitionId -> mapPartitionId -> attemptId
  private final Table<String, Integer, AtomicInteger> taskAttemptsTable = HashBasedTable.create();

  public int getRssAttemptId(String partitionId, int mapPartitionId) {
    if (taskAttemptsTable.contains(partitionId, mapPartitionId)) {
      AtomicInteger atomicInteger = taskAttemptsTable.get(partitionId, mapPartitionId);
      assert atomicInteger != null;
      return atomicInteger.getAndIncrement();
    } else {
      AtomicInteger atomicInteger = new AtomicInteger(0);
      int rssPartitionId = atomicInteger.getAndIncrement();
      taskAttemptsTable.put(partitionId, mapPartitionId, atomicInteger);
      return rssPartitionId;
    }
  }
}
