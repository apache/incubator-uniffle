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

package org.apache.uniffle.common.util;

// BlockId is positive long (63 bits) composed of partitionId, taskAttemptId and AtomicInteger.
// AtomicInteger is highest 18 bits, max value is 2^18 - 1
// partitionId is middle 24 bits, max value is 2^24 - 1
// taskAttemptId is lowest 21 bits, max value is 2^21 - 1
// Values of partitionId, taskAttemptId and AtomicInteger are always positive.
public class BlockId {
  public final long blockId;
  public final int sequenceNo;
  public final int partitionId;
  public final int taskAttemptId;

  private BlockId(long blockId, int sequenceNo, int partitionId, int taskAttemptId) {
    this.blockId = blockId;
    this.sequenceNo = sequenceNo;
    this.partitionId = partitionId;
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public String toString() {
    return "blockId["
        + Long.toHexString(blockId)
        + " (seq: "
        + sequenceNo
        + ", part: "
        + partitionId
        + ", task: "
        + taskAttemptId
        + ")]";
  }

  public static String toString(long blockId) {
    return BlockId.fromLong(blockId).toString();
  }

  public static BlockId fromLong(long blockId) {
    int sequenceNo = getSequenceNo(blockId);
    int partitionId = getPartitionId(blockId);
    int taskAttemptId = getTaskAttemptId(blockId);
    return new BlockId(blockId, sequenceNo, partitionId, taskAttemptId);
  }

  public static BlockId fromIds(int sequenceNo, int partitionId, int taskAttemptId) {
    long blockId = getBlockId(sequenceNo, partitionId, taskAttemptId);
    return new BlockId(blockId, sequenceNo, partitionId, taskAttemptId);
  }

  public static long getBlockId(int sequenceNo, int partitionId, long taskAttemptId) {
    if (sequenceNo < 0 || sequenceNo > Constants.MAX_SEQUENCE_NO) {
      throw new IllegalArgumentException(
          "Can't support sequence["
              + sequenceNo
              + "], the max value should be "
              + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new IllegalArgumentException(
          "Can't support partitionId["
              + partitionId
              + "], the max value should be "
              + Constants.MAX_PARTITION_ID);
    }
    if (taskAttemptId < 0 || taskAttemptId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new IllegalArgumentException(
          "Can't support taskAttemptId["
              + taskAttemptId
              + "], the max value should be "
              + Constants.MAX_TASK_ATTEMPT_ID);
    }

    return ((long) sequenceNo
            << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + ((long) partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH)
        + taskAttemptId;
  }

  public static int getSequenceNo(long blockId) {
    return (int)
        (blockId >> (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH));
  }

  public static int getPartitionId(long blockId) {
    return (int) ((blockId >> Constants.TASK_ATTEMPT_ID_MAX_LENGTH) & Constants.MAX_PARTITION_ID);
  }

  public static int getTaskAttemptId(long blockId) {
    return (int) (blockId & Constants.MAX_TASK_ATTEMPT_ID);
  }
}
