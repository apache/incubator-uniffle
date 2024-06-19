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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.stage.RssShuffleStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssStageResubmitManagerTest {

  @Test
  public void testMultipleStatusFromWriterAndReader() {
    int shuffleId = 1;
    int stageId = 10;
    RssStageResubmitManager manager = new RssStageResubmitManager(new SparkConf());
    RssShuffleStatus readerShuffleStatus = manager.getShuffleStatusForReader(shuffleId, stageId, 0);

    // case1
    readerShuffleStatus.incTaskFailure(0);
    readerShuffleStatus.incTaskFailure(1);
    readerShuffleStatus.incTaskFailure(2);
    assertFalse(manager.activateStageRetry(readerShuffleStatus));

    readerShuffleStatus.incTaskFailure(3);
    assertTrue(manager.activateStageRetry(readerShuffleStatus));

    readerShuffleStatus.markStageAttemptRetried();
    assertTrue(manager.isStageAttemptRetried(shuffleId, stageId, 0));
    assertFalse(manager.isStageAttemptRetried(shuffleId, stageId, 1));

    readerShuffleStatus = manager.getShuffleStatusForReader(shuffleId, stageId, 1);

    // case2
    RssShuffleStatus writerShuffleStatus = manager.getShuffleStatusForWriter(shuffleId, stageId, 1);
    writerShuffleStatus.incTaskFailure(0);
    writerShuffleStatus.incTaskFailure(1);
    readerShuffleStatus.incTaskFailure(0);
    readerShuffleStatus.incTaskFailure(1);
    assertFalse(manager.activateStageRetry(readerShuffleStatus));
    assertFalse(manager.activateStageRetry(writerShuffleStatus));

    writerShuffleStatus.incTaskFailure(2);
    writerShuffleStatus.incTaskFailure(3);
    if (manager.activateStageRetry(writerShuffleStatus)) {
      writerShuffleStatus.markStageAttemptRetried();
    }
    assertTrue(manager.isStageAttemptRetried(shuffleId, stageId, 1));
  }
}
