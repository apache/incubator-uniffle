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

import java.util.Objects;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

/**
 * This represents the actual bit layout of {@link BlockId}s.
 *
 * <p>BlockId is positive long (63 bits) composed of sequenceNo, partitionId and taskAttemptId in
 * that order from highest to lowest bits. The number of bits is defined by a {@link BlockIdLayout}.
 * Values of partitionId, taskAttemptId and AtomicInteger are always positive.
 */
public class BlockIdLayout {

  // historic default values, client-specific config defaults may vary
  // see RssSparkConfig.RSS_MAX_PARTITIONS
  public static final BlockIdLayout DEFAULT = BlockIdLayout.from(18, 24, 21);

  public final int sequenceNoBits;
  public final int partitionIdBits;
  public final int taskAttemptIdBits;

  public final int sequenceNoOffset;
  public final int partitionIdOffset;
  public final int taskAttemptIdOffset;

  public final long sequenceNoMask;
  public final long partitionIdMask;
  public final long taskAttemptIdMask;

  public final int maxSequenceNo;
  public final int maxPartitionId;
  public final int maxTaskAttemptId;

  public final int maxNumPartitions;

  private BlockIdLayout(int sequenceNoBits, int partitionIdBits, int taskAttemptIdBits) {
    // individual lengths must be lager than 0
    if (sequenceNoBits <= 0 || partitionIdBits <= 0 || taskAttemptIdBits <= 0) {
      throw new IllegalArgumentException(
          "Don't support given lengths, individual lengths must be larger than 0: given "
              + "sequenceNoBits="
              + sequenceNoBits
              + ", partitionIdBits="
              + partitionIdBits
              + ", taskAttemptIdBits="
              + taskAttemptIdBits);
    }
    // individual lengths must be less than 32
    if (sequenceNoBits >= 32 || partitionIdBits >= 32 || taskAttemptIdBits >= 32) {
      throw new IllegalArgumentException(
          "Don't support given lengths, individual lengths must be less that 32: given "
              + "sequenceNoBits="
              + sequenceNoBits
              + ", partitionIdBits="
              + partitionIdBits
              + ", taskAttemptIdBits="
              + taskAttemptIdBits);
    }
    // sum of individual lengths must be 63, otherwise we waste bits and risk overflow
    if (sequenceNoBits + partitionIdBits + taskAttemptIdBits != 63) {
      throw new IllegalArgumentException(
          "Don't support given lengths, sum must be exactly 63: "
              + sequenceNoBits
              + " + "
              + partitionIdBits
              + " + "
              + taskAttemptIdBits
              + " = "
              + (sequenceNoBits + partitionIdBits + taskAttemptIdBits));
    }

    this.sequenceNoBits = sequenceNoBits;
    this.partitionIdBits = partitionIdBits;
    this.taskAttemptIdBits = taskAttemptIdBits;

    // fields are right-aligned
    this.sequenceNoOffset = partitionIdBits + taskAttemptIdBits;
    this.partitionIdOffset = taskAttemptIdBits;
    this.taskAttemptIdOffset = 0;

    // compute max ids
    this.maxSequenceNo = (1 << sequenceNoBits) - 1;
    this.maxPartitionId = (1 << partitionIdBits) - 1;
    this.maxTaskAttemptId = (1 << taskAttemptIdBits) - 1;

    // compute max nums
    this.maxNumPartitions = this.maxPartitionId + 1;

    // compute masks to simplify bit logic in BlockId methods
    this.sequenceNoMask = (long) maxSequenceNo << sequenceNoOffset;
    this.partitionIdMask = (long) maxPartitionId << partitionIdOffset;
    this.taskAttemptIdMask = (long) maxTaskAttemptId << taskAttemptIdOffset;
  }

  @Override
  public String toString() {
    return "blockIdLayout["
        + "seq: "
        + sequenceNoBits
        + " bits, part: "
        + partitionIdBits
        + " bits, task: "
        + taskAttemptIdBits
        + " bits]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockIdLayout that = (BlockIdLayout) o;
    return sequenceNoBits == that.sequenceNoBits
        && partitionIdBits == that.partitionIdBits
        && taskAttemptIdBits == that.taskAttemptIdBits;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sequenceNoBits, partitionIdBits, taskAttemptIdBits);
  }

  public long getBlockId(int sequenceNo, int partitionId, long taskAttemptId) {
    if (sequenceNo < 0 || sequenceNo > maxSequenceNo) {
      throw new IllegalArgumentException(
          "Don't support sequenceNo[" + sequenceNo + "], the max value should be " + maxSequenceNo);
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

  public BlockId asBlockId(int sequenceNo, int partitionId, long taskAttemptId) {
    return new BlockId(
        getBlockId(sequenceNo, partitionId, taskAttemptId),
        this,
        sequenceNo,
        partitionId,
        (int) taskAttemptId);
  }

  public static BlockIdLayout from(RssConf rssConf) {
    int sequenceBits = rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS);
    int partitionBits = rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS);
    int attemptBits = rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS);
    return BlockIdLayout.from(sequenceBits, partitionBits, attemptBits);
  }

  public static BlockIdLayout from(int sequenceNoBits, int partitionIdBits, int taskAttemptIdBits) {
    return new BlockIdLayout(sequenceNoBits, partitionIdBits, taskAttemptIdBits);
  }
}
