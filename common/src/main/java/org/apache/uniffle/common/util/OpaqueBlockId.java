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

public class OpaqueBlockId implements BlockId {
  public final long blockId;

  public OpaqueBlockId(long blockId) {
    this.blockId = blockId;
  }

  @Override
  public long getBlockId() {
    return blockId;
  }

  public BlockIdWithLayout withLayout(BlockIdLayout layout) {
    return layout.asBlockId(blockId);
  }

  @Override
  public int getSequenceNo() {
    throw new UnsupportedOperationException(
        "This is an opaque block id, "
            + "the sequence number cannot be accessed as the layout is not known.");
  }

  @Override
  public int getPartitionId() {
    throw new UnsupportedOperationException(
        "This is an opaque block id, "
            + "the partition id cannot be accessed as the layout is not known.");
  }

  @Override
  public int getTaskAttemptId() {
    throw new UnsupportedOperationException(
        "This is an opaque block id, "
            + "the task attempt id cannot be accessed as the layout is not known.");
  }

  @Override
  public String toString() {
    return Long.toHexString(blockId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof BlockId) {
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
