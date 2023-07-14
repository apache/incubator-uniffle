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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TezIdHelperTest {

  @Test
  public void testTetTaskAttemptId() {
    TezIdHelper tezIdHelper = new TezIdHelper();
    assertEquals(0, tezIdHelper.getTaskAttemptId(27262976));
    assertEquals(1, tezIdHelper.getTaskAttemptId(27262977));
    assertEquals(
        0, RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
    assertEquals(
        tezIdHelper.getTaskAttemptId(27262976),
        RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
  }
}
