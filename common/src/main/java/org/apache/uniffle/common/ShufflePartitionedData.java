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

package org.apache.uniffle.common;

import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Triple;

public class ShufflePartitionedData {

  private static final ShufflePartitionedBlock[] EMPTY_BLOCK_LIST =
      new ShufflePartitionedBlock[] {};
  private int partitionId;
  private final ShufflePartitionedBlock[] blockList;
  private final long totalBlockSize;
  private final long totalBlockLength;

  public ShufflePartitionedData(
      int partitionId, Triple<Long, Long, ShufflePartitionedBlock[]> pair) {
    this.partitionId = partitionId;
    this.blockList = pair.getRight() == null ? EMPTY_BLOCK_LIST : pair.getRight();
    totalBlockSize = pair.getLeft();
    totalBlockLength = pair.getMiddle();
  }

  @VisibleForTesting
  public ShufflePartitionedData(int partitionId, ShufflePartitionedBlock[] blockList) {
    this.partitionId = partitionId;
    this.blockList = blockList == null ? EMPTY_BLOCK_LIST : blockList;
    long size = 0L;
    long length = 0L;
    for (ShufflePartitionedBlock block : this.blockList) {
      size += block.getSize();
      length += block.getLength();
    }
    totalBlockSize = size;
    totalBlockLength = length;
  }

  @Override
  public String toString() {
    return "ShufflePartitionedData{partitionId="
        + partitionId
        + ", blockList="
        + Arrays.toString(blockList)
        + '}';
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public ShufflePartitionedBlock[] getBlockList() {
    return blockList;
  }

  public long getTotalBlockSize() {
    return totalBlockSize;
  }

  public long getTotalBlockLength() {
    return totalBlockLength;
  }
}
