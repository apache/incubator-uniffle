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

package org.apache.uniffle.shuffle.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.uniffle.shuffle.writer.RssShuffleOutputGate;

public class RssShuffleResultPartition extends ResultPartition {

  private RssShuffleResultPartitionPlugin plugin;

  public RssShuffleResultPartition(
      String owningTaskName,
      int partitionIndex,
      ResultPartitionID partitionId,
      ResultPartitionType partitionType,
      int numSubpartitions,
      int numTargetKeyGroups,
      int networkBufferSize,
      ResultPartitionManager partitionManager,
      @Nullable BufferCompressor bufferCompressor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      RssShuffleOutputGate outputGate) {
    super(
        owningTaskName,
        partitionIndex,
        partitionId,
        partitionType,
        numSubpartitions,
        numTargetKeyGroups,
        partitionManager,
        bufferCompressor,
        bufferPoolFactory);
    plugin =
        new RssShuffleResultPartitionPlugin(
            owningTaskName,
            partitionIndex,
            partitionId,
            partitionType,
            numSubpartitions,
            numTargetKeyGroups,
            networkBufferSize,
            partitionManager,
            bufferCompressor,
            bufferPoolFactory,
            outputGate);
  }

  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    plugin.emitRecord(record, targetSubpartition);
  }

  @Override
  public void broadcastRecord(ByteBuffer record) throws IOException {
    plugin.broadcastRecord(record);
  }

  @Override
  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    plugin.broadcastEvent(event, isPriorityEvent);
  }

  @Override
  public ResultSubpartitionView createSubpartitionView(
      int index, BufferAvailabilityListener availabilityListener) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void flushAll() {
    plugin.flushAll();
  }

  @Override
  public void flush(int subpartitionIndex) {
    plugin.flush(subpartitionIndex);
  }

  // ------------------------------------------------------------------------
  // There is currently no implementation method.
  // ------------------------------------------------------------------------

  @Override
  protected void releaseInternal() {}

  @Override
  public int getNumberOfQueuedBuffers() {
    return 0;
  }

  @Override
  public int getNumberOfQueuedBuffers(int targetSubpartition) {
    return 0;
  }
}
