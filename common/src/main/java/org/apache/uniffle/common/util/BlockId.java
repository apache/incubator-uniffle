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

/**
 * This represents a block id and all its constituents. This is particularly useful for logging and
 * debugging block ids.
 *
 * <p>BlockId is positive long (63 bits) composed of sequenceNo, partitionId and taskAttemptId in
 * that order from highest to lowest bits. The number of bits is defined by a {@link BlockIdLayout}.
 * Values of partitionId, taskAttemptId and AtomicInteger are always positive.
 */
public interface BlockId extends Comparable<BlockId> {
  long getBlockId();

  int getSequenceNo();

  int getPartitionId();

  int getTaskAttemptId();

  default BlockId withLayoutIfOpaque(BlockIdLayout layout) {
    if (this instanceof OpaqueBlockId) {
      return ((OpaqueBlockId) this).withLayout(layout);
    }
    return this;
  }

  @Override
  default int compareTo(BlockId other) {
    return Long.compare(this.getBlockId(), other.getBlockId());
  }
}
