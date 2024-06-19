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

package org.apache.spark.shuffle.stage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RssShuffleStatusTest {

  @Test
  public void test() {
    // case1
    RssShuffleStatus shuffleStatus = new RssShuffleStatus(10, 1);
    shuffleStatus.updateStageAttemptIfNecessary(0);
    shuffleStatus.incTaskFailure(0);
    shuffleStatus.incTaskFailure(1);
    assertEquals(0, shuffleStatus.getStageAttempt());
    assertEquals(2, shuffleStatus.getTaskFailureAttemptCount());

    // case2
    shuffleStatus.updateStageAttemptIfNecessary(1);
    assertEquals(1, shuffleStatus.getStageAttempt());
    assertEquals(0, shuffleStatus.getTaskFailureAttemptCount());
    shuffleStatus.incTaskFailure(1);
    shuffleStatus.incTaskFailure(3);
    shuffleStatus.incTaskFailure(2);
    assertEquals(3, shuffleStatus.getTaskFailureAttemptCount());

    // case3
    shuffleStatus.markStageAttemptRetried();
    assertEquals(1, shuffleStatus.getStageRetriedNumber());

    // case4: illegal stage attempt
    assertFalse(shuffleStatus.updateStageAttemptIfNecessary(0));
  }
}
