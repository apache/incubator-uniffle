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

package org.apache.uniffle.flink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.uniffle.flink.partition.RssShuffleResultPartitionCommon;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.flink.writer.RssShuffleOutputGateCommon;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.uniffle.flink.buffer.WriterBuffer.HEADER_LENGTH;

public class RssShuffleResultPartition extends ResultPartition {

  private final RssShuffleResultPartitionCommon common;
  private final RssShuffleOutputGateCommon outputGate;

  public RssShuffleResultPartition(
      String owningTaskName,
      int partitionIndex,
      ResultPartitionID partitionId,
      ResultPartitionType partitionType,
      int numSubpartitions,
      int totalNumberOfPartitions,
      int numTargetKeyGroups,
      int networkBufferSize,
      ResultPartitionManager partitionManager,
      @Nullable BufferCompressor bufferCompressor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      RssShuffleDescriptor rsd,
      Configuration config) {
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
    outputGate =
        new RssShuffleOutputGateCommon(
            rsd, networkBufferSize, bufferPoolFactory, config, totalNumberOfPartitions);
    common =
        new RssShuffleResultPartitionCommon(
            networkBufferSize, outputGate, this::updateStatistics, numSubpartitions);
  }

  @Override
  public void setup() throws IOException {
    super.setup();

    common.setup(bufferPool, bufferCompressor, this::canBeCompressed, this::checkInProduceState);
  }

  private void updateStatistics(Buffer buffer, boolean isBroadcast) {
    numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
    long readableBytes = buffer.readableBytes() - HEADER_LENGTH;
    numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
  }

  // ----- 不需要的方法，可以参考SortMergeResultPartition
  // DONE
  @Override
  public int getNumberOfQueuedBuffers() {
    return 0;
  }

  // DONE
  @Override
  public long getSizeOfQueuedBuffersUnsafe() {
    return 0;
  }

  // DONE
  @Override
  public int getNumberOfQueuedBuffers(int targetSubpartition) {
    return 0;
  }

  // DONE
  @Override
  protected void releaseInternal() {}

  // DONE
  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    common.emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
  }

  // DONE
  @Override
  public void broadcastRecord(ByteBuffer record) throws IOException {
    common.broadcast(record, Buffer.DataType.DATA_BUFFER);
  }

  // DONE
  @Override
  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
    try {
      ByteBuffer serializedEvent = buffer.getNioBufferReadable();
      common.broadcast(serializedEvent, buffer.getDataType());
    } finally {
      buffer.recycleBuffer();
    }
  }

  // DONE
  @Override
  public ResultSubpartitionView createSubpartitionView(
      int index, BufferAvailabilityListener availabilityListener) throws IOException {
    throw new UnsupportedOperationException("RSS Not supported.");
  }

  // DONE
  @Override
  public void flushAll() {
    common.flushAll();
  }

  // DONE
  @Override
  public void flush(int subpartitionIndex) {
    flushAll();
  }

  // DONE
  @Override
  public CompletableFuture<?> getAvailableFuture() {
    return AVAILABLE;
  }

  // DONE
  @Override
  public boolean isAvailable() {
    return super.isAvailable();
  }

  // DONE
  @Override
  public boolean isApproximatelyAvailable() {
    return super.isApproximatelyAvailable();
  }

  // DONE
  @Override
  public void finish() throws IOException {
    checkState(!isReleased(), "Result partition is already released.");
    broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
    common.finish();
    super.finish();
  }

  // DONE
  @Override
  public synchronized void close() {
    common.close(super::close);
  }
}
