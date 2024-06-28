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

package org.apache.uniffle.test.listener;

import java.util.HashMap;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

public class WriteAndReadMetricsSparkListener extends SparkListener {
  private HashMap<Integer, Long> stageIdToWriteRecords = new HashMap<>();
  private HashMap<Integer, Long> stageIdToReadRecords = new HashMap<>();

  @Override
  public void onTaskEnd(SparkListenerTaskEnd event) {
    int stageId = event.stageId();
    TaskMetrics taskMetrics = event.taskMetrics();
    if (taskMetrics != null) {
      long writeRecords = taskMetrics.shuffleWriteMetrics().recordsWritten();
      long readRecords = taskMetrics.shuffleReadMetrics().recordsRead();
      // Accumulate writeRecords and readRecords for the given stageId
      stageIdToWriteRecords.put(
          stageId, stageIdToWriteRecords.getOrDefault(stageId, 0L) + writeRecords);
      stageIdToReadRecords.put(
          stageId, stageIdToReadRecords.getOrDefault(stageId, 0L) + readRecords);
    }
  }

  public long getWriteRecords(int stageId) {
    return stageIdToWriteRecords.getOrDefault(stageId, 0L);
  }

  public long getReadRecords(int stageId) {
    return stageIdToReadRecords.getOrDefault(stageId, 0L);
  }
}
