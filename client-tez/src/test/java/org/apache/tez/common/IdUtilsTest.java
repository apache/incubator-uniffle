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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdUtilsTest {

  @Test
  public void testConvertTezTaskAttemptID() {
    ApplicationId appId = ApplicationId.newInstance(1681717153064L, 2768836);
    TezDAGID dagId = TezDAGID.getInstance(appId, 3);
    TezVertexID vId = TezVertexID.getInstance(dagId, 2);
    TezTaskID taskId = TezTaskID.getInstance(vId, 1);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);

    String testId = "attempt_1681717153064_2768836_3_02_000001_0_10006";
    assertEquals(tezTaskAttemptId, IdUtils.convertTezTaskAttemptID(testId));
  }
}
