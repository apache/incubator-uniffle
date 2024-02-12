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

import java.util.Map;

import org.apache.uniffle.common.config.RssConf;

/**
 * This represents the actual bit layout of {@link BlockId}s.
 *
 * <p>BlockId is positive long (63 bits) composed of sequenceNo, partitionId and taskAttemptId in
 * that order from highest to lowest bits. The number of bits is defined by a {@link BlockIdLayout}.
 * Values of partitionId, taskAttemptId and AtomicInteger are always positive.
 */
public class BlockIdLayout {
  public static BlockIdLayout DEFAULT = BlockIdLayoutConfig.apply(BlockIdLayoutConfig.DEFAULT);

  public final int sequenceNoLength;
  public final int partitionIdLength;
  public final int taskAttemptIdLength;

  public final int sequenceNoOffset;
  public final int partitionIdOffset;
  public final int taskAttemptIdOffset;

  public final long sequenceNoMask;
  public final long partitionIdMask;
  public final long taskAttemptIdMask;

  public final int maxSequenceNo;
  public final int maxPartitionId;
  public final int maxTaskAttemptId;

  private BlockIdLayout(int sequenceNoLength, int partitionIdLength, int taskAttemptIdLength) {
    // individual lengths must be lager than 0
    if (sequenceNoLength <= 0 || partitionIdLength <= 0 || taskAttemptIdLength <= 0) {
      throw new IllegalArgumentException(
          "Don't support given lengths, individual lengths must be larger than 0: given "
              + "sequenceNoLength="
              + sequenceNoLength
              + ", "
              + "partitionIdLength="
              + partitionIdLength
              + ", "
              + "taskAttemptIdLength="
              + taskAttemptIdLength);
    }
    // individual lengths must be less than 32
    if (sequenceNoLength >= 32 || partitionIdLength >= 32 || taskAttemptIdLength >= 32) {
      throw new IllegalArgumentException(
          "Don't support given lengths, individual lengths must be less that 32: given "
              + "sequenceNoLength="
              + sequenceNoLength
              + ", "
              + "partitionIdLength="
              + partitionIdLength
              + ", "
              + "taskAttemptIdLength="
              + taskAttemptIdLength);
    }
    // sum of individual lengths must 63, otherwise we waste bits and risk overflow
    if (sequenceNoLength + partitionIdLength + taskAttemptIdLength != 63) {
      throw new IllegalArgumentException(
          "Don't support given lengths, sum must be exactly 63: "
              + sequenceNoLength
              + " + "
              + partitionIdLength
              + " + "
              + taskAttemptIdLength
              + " = "
              + (sequenceNoLength + partitionIdLength + taskAttemptIdLength));
    }

    this.sequenceNoLength = sequenceNoLength;
    this.partitionIdLength = partitionIdLength;
    this.taskAttemptIdLength = taskAttemptIdLength;

    // fields are right-aligned
    this.sequenceNoOffset = partitionIdLength + taskAttemptIdLength;
    this.partitionIdOffset = taskAttemptIdLength;
    this.taskAttemptIdOffset = 0;

    // compute max ids
    this.maxSequenceNo = (1 << sequenceNoLength) - 1;
    this.maxPartitionId = (1 << partitionIdLength) - 1;
    this.maxTaskAttemptId = (1 << taskAttemptIdLength) - 1;

    // compute masks to simplify bit logic in BlockId methods
    this.sequenceNoMask = (long) maxSequenceNo << sequenceNoOffset;
    this.partitionIdMask = (long) maxPartitionId << partitionIdOffset;
    this.taskAttemptIdMask = (long) maxTaskAttemptId << taskAttemptIdOffset;
  }

  @Override
  public String toString() {
    return "blockIdLayout["
        + "seq: "
        + sequenceNoLength
        + " bits, part: "
        + partitionIdLength
        + " bits, task: "
        + taskAttemptIdLength
        + " bits]";
  }

  public long getBlockId(int sequenceNo, int partitionId, long taskAttemptId) {
    if (sequenceNo < 0 || sequenceNo > maxSequenceNo) {
      throw new IllegalArgumentException(
          "Don't support sequence[" + sequenceNo + "], the max value should be " + maxSequenceNo);
    }
    if (partitionId < 0 || partitionId > maxPartitionId) {
      throw new IllegalArgumentException(
          "Don't support partitionId["
              + partitionId
              + "], the max value should be "
              + maxPartitionId);
    }
    if (taskAttemptId < 0 || taskAttemptId > maxTaskAttemptId) {
      throw new IllegalArgumentException(
          "Don't support taskAttemptId["
              + taskAttemptId
              + "], the max value should be "
              + maxTaskAttemptId);
    }

    return (long) sequenceNo << sequenceNoOffset
        | (long) partitionId << partitionIdOffset
        | taskAttemptId << taskAttemptIdOffset;
  }

  public int getSequenceNo(long blockId) {
    return (int) ((blockId & sequenceNoMask) >> sequenceNoOffset);
  }

  public int getPartitionId(long blockId) {
    return (int) ((blockId & partitionIdMask) >> partitionIdOffset);
  }

  public int getTaskAttemptId(long blockId) {
    return (int) ((blockId & taskAttemptIdMask) >> taskAttemptIdOffset);
  }

  public BlockId asBlockId(long blockId) {
    return new BlockId(
        blockId, this, getSequenceNo(blockId), getPartitionId(blockId), getTaskAttemptId(blockId));
  }

  public BlockId asBlockId(int sequenceNo, int partitionId, int taskAttemptId) {
    return new BlockId(
        getBlockId(sequenceNo, partitionId, taskAttemptId),
        this,
        sequenceNo,
        partitionId,
        taskAttemptId);
  }

  public static BlockIdLayout from(RssConf config) {
    return BlockIdLayoutConfig.apply(config);
  }

  public static BlockIdLayout from(Map<String, String> config) {
    return BlockIdLayoutConfig.apply(config);
  }

  public static BlockIdLayout from(
      int sequenceNoLength, int partitionIdLength, int taskAttemptIdLength) {
    return new BlockIdLayout(sequenceNoLength, partitionIdLength, taskAttemptIdLength);
  }
}
