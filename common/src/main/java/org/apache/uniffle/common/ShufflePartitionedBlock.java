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

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

public class ShufflePartitionedBlock {

  private int length;
  private long crc;
  private long blockId;
  private int uncompressLength;
  private long taskAttemptId;
  // Read only byte buffer
  private ByteBuffer bufferData;

  public ShufflePartitionedBlock(
      int length,
      int uncompressLength,
      long crc,
      long blockId,
      ByteBuffer data,
      long taskAttemptId) {
    this.length = length;
    this.crc = crc;
    this.blockId = blockId;
    this.uncompressLength = uncompressLength;
    this.taskAttemptId = taskAttemptId;
    this.bufferData = data;
  }

  // Only for tests
  @VisibleForTesting
  public ShufflePartitionedBlock(
      int length,
      int uncompressLength,
      long crc,
      long blockId,
      long taskAttemptId,
      byte[] data) {
    this(length, uncompressLength, crc, blockId, data == null ? null : ByteBuffer.wrap(data), taskAttemptId);
  }

  // calculate the data size for this block in memory including metadata which are
  // blockId, crc, taskAttemptId, length, uncompressLength
  public long getSize() {
    return length + 3 * 8 + 2 * 4;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShufflePartitionedBlock that = (ShufflePartitionedBlock) o;
    return length == that.length
        && crc == that.crc
        && blockId == that.blockId
        && bufferData.equals(that.bufferData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(length, crc, blockId, bufferData.hashCode());
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public byte[] getData() {
    return bufferData.array();
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(long taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  public ByteBuffer getBufferData() {
    return bufferData;
  }

  @Override
  public String toString() {
    return "ShufflePartitionedBlock{blockId[" + blockId + "], length[" + length
        + "], uncompressLength[" + uncompressLength + "], crc[" + crc + "], taskAttemptId[" + taskAttemptId + "]}";
  }
}
