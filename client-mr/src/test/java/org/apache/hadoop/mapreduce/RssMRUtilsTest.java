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

package org.apache.hadoop.mapreduce;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RssMRUtilsTest {

  @Test
  public void TaskAttemptIdTest() {
    long taskAttemptId = 0x1000ad12;
    TaskAttemptID mrTaskAttemptId = RssMRUtils.createMRTaskAttemptId(new org.apache.hadoop.mapred.JobID(), TaskType.MAP, taskAttemptId);
    long testId = RssMRUtils.convertTaskAttemptIdToLong(mrTaskAttemptId);
    assertEquals(taskAttemptId, testId);
    taskAttemptId = 0xff1000ad12L;
    mrTaskAttemptId = RssMRUtils.createMRTaskAttemptId(new JobID(), TaskType.MAP, taskAttemptId);
    testId = RssMRUtils.convertTaskAttemptIdToLong(mrTaskAttemptId);
    assertEquals(taskAttemptId, testId);
  }
}
