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

/**
 * This represents a block id and all its constituents. This is particularly useful for logging and
 * debugging block ids.
 *
 * <p>BlockId is positive long (63 bits) composed of sequenceNo, partitionId and taskAttemptId in
 * that order from highest to lowest bits. The number of bits is defined by a {@link BlockIdLayout}.
 * Values of partitionId, taskAttemptId and AtomicInteger are always positive.
 */
public class BlockIdWithLayout implements BlockId {
  public final long blockId;
  public final BlockIdLayout layout;
  public final int sequenceNo;
  public final int partitionId;
  public final int taskAttemptId;

  protected BlockIdWithLayout(
      long blockId, BlockIdLayout layout, int sequenceNo, int partitionId, int taskAttemptId) {
    this.blockId = blockId;
    this.layout = layout;
    this.sequenceNo = sequenceNo;
    this.partitionId = partitionId;
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public long getBlockId() {
    return blockId;
  }

  @Override
  public int getSequenceNo() {
    return sequenceNo;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public int getTaskAttemptId() {
    return taskAttemptId;
  }

  @Override
  public String toString() {
    return Long.toHexString(blockId)
        + " (seq: "
        + sequenceNo
        + ", part: "
        + partitionId
        + ", task: "
        + taskAttemptId
        + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof BlockIdWithLayout) {
      BlockIdWithLayout other = (BlockIdWithLayout) o;
      return blockId == other.blockId && Objects.equals(layout, other.layout);
    }
    if (o instanceof OpaqueBlockId) {
      BlockId other = (BlockId) o;
      return blockId == other.getBlockId();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockId);
  }
}
