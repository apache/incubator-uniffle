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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.shuffle.buffer.BufferWithChannel;
import org.apache.uniffle.shuffle.buffer.SortBasedWriteBuffer;
import org.apache.uniffle.shuffle.buffer.WriteBuffer;
import org.apache.uniffle.shuffle.exception.ShuffleException;
import org.apache.uniffle.shuffle.utils.BufferUtils;
import org.apache.uniffle.shuffle.utils.ExceptionUtils;
import org.apache.uniffle.shuffle.writer.RssShuffleOutputGate;

import static org.apache.uniffle.shuffle.utils.CommonUtils.checkNotNull;
import static org.apache.uniffle.shuffle.utils.CommonUtils.checkState;
import static org.apache.uniffle.shuffle.utils.ExceptionUtils.translateToRuntimeException;

public class RssShuffleResultPartition extends ResultPartition {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleResultPartition.class);

  /** Size of network buffer and write buffer. */
  private final int networkBufferSize;

  /** {@link WriteBuffer} for records sent by {@link #broadcastRecord(ByteBuffer)}. */
  private WriteBuffer broadcastWriteBuffer;

  /** {@link WriteBuffer} for records sent by {@link #emitRecord(ByteBuffer, int)}. */
  private WriteBuffer unicastWriteBuffer;

  /** Utility to spill data to shuffle workers. */
  private final RssShuffleOutputGate outputGate;

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
    this.outputGate = outputGate;
    this.networkBufferSize = networkBufferSize;
  }

  @Override
  public void setup() throws IOException {
    super.setup();
    BufferUtils.bufferReservationForRequirements(bufferPool, 1);
    try {
      outputGate.setup();
    } catch (Throwable throwable) {
      LOG.error("Failed to setup remote output gate.", throwable);
      translateToRuntimeException(throwable);
    }
  }

  @Override
  public int getNumberOfQueuedBuffers() {
    return 0;
  }

  @Override
  public int getNumberOfQueuedBuffers(int targetSubpartition) {
    return 0;
  }

  @Override
  protected void releaseInternal() {
    // no-operator
  }

  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
  }

  private void emit(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws IOException {

    checkInProduceState();
    if (isBroadcast) {
      Preconditions.checkState(
          targetSubpartition == 0, "Target subpartition index can only be 0 when broadcast.");
    }

    WriteBuffer sortBuffer = isBroadcast ? getBroadcastWriteBuffer() : getUnicastWriteBuffer();
    if (sortBuffer.append(record, targetSubpartition, dataType)) {
      return;
    }

    try {
      if (!sortBuffer.hasRemaining()) {
        // the record can not be appended to the free sort buffer because it is too large
        sortBuffer.finish();
        sortBuffer.release();
        writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
        return;
      }
      flushWriteBuffer(sortBuffer, isBroadcast);
    } catch (InterruptedException e) {
      LOG.error("Failed to flush the sort buffer.", e);
      ExceptionUtils.translateToRuntimeException(e);
    }
    emit(record, targetSubpartition, dataType, isBroadcast);
  }

  @Override
  public void broadcastRecord(ByteBuffer record) throws IOException {
    broadcast(record, Buffer.DataType.DATA_BUFFER);
  }

  private void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
    emit(record, 0, dataType, true);
  }

  @Override
  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
    try {
      ByteBuffer serializedEvent = buffer.getNioBufferReadable();
      broadcast(serializedEvent, buffer.getDataType());
    } finally {
      buffer.recycleBuffer();
    }
  }

  @Override
  public ResultSubpartitionView createSubpartitionView(
      int index, BufferAvailabilityListener availabilityListener) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void flushAll() {
    try {
      flushUnicastWriteBuffer();
      flushBroadcastWriteBuffer();
    } catch (Throwable t) {
      LOG.error("Failed to flush the current sort buffer.", t);
      ExceptionUtils.translateToRuntimeException(t);
    }
  }

  @Override
  public void flush(int subpartitionIndex) {
    flushAll();
  }

  private void writeLargeRecord(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws InterruptedException {

    outputGate.regionStart(isBroadcast);
    while (record.hasRemaining()) {
      MemorySegment writeBuffer = outputGate.getBufferPool().requestMemorySegmentBlocking();
      int toCopy = Math.min(record.remaining(), writeBuffer.size() - BufferUtils.HEADER_LENGTH);
      writeBuffer.put(BufferUtils.HEADER_LENGTH, record, toCopy);
      NetworkBuffer buffer =
          new NetworkBuffer(
              writeBuffer,
              outputGate.getBufferPool(),
              dataType,
              toCopy + BufferUtils.HEADER_LENGTH);

      updateStatistics(buffer, isBroadcast);
      writeCompressedBufferIfPossible(buffer, targetSubpartition);
    }
    outputGate.regionFinish();
  }

  private WriteBuffer getUnicastWriteBuffer() throws IOException {
    flushBroadcastWriteBuffer();

    if (unicastWriteBuffer != null && !unicastWriteBuffer.isFinished()) {
      return unicastWriteBuffer;
    }

    // todo:Need to confirm numGuaranteedBuffers size
    unicastWriteBuffer =
        new SortBasedWriteBuffer(bufferPool, numSubpartitions, networkBufferSize, 1024, null);
    return unicastWriteBuffer;
  }

  private void flushBroadcastWriteBuffer() throws IOException {
    flushWriteBuffer(broadcastWriteBuffer, true);
  }

  private WriteBuffer getBroadcastWriteBuffer() throws IOException {
    flushUnicastWriteBuffer();

    if (broadcastWriteBuffer != null && !broadcastWriteBuffer.isFinished()) {
      return broadcastWriteBuffer;
    }

    // todo:Need to confirm numGuaranteedBuffers size
    broadcastWriteBuffer =
        new SortBasedWriteBuffer(bufferPool, numSubpartitions, networkBufferSize, 1024, null);
    return broadcastWriteBuffer;
  }

  private void flushUnicastWriteBuffer() throws IOException {
    flushWriteBuffer(unicastWriteBuffer, false);
  }

  private void flushWriteBuffer(WriteBuffer writeBuffer, boolean isBroadcast) throws IOException {
    if (writeBuffer == null || writeBuffer.isReleased()) {
      return;
    }
    writeBuffer.finish();
    if (writeBuffer.hasRemaining()) {
      try {
        outputGate.regionStart(isBroadcast);
        while (writeBuffer.hasRemaining()) {
          MemorySegment segment = outputGate.getBufferPool().requestMemorySegmentBlocking();
          BufferWithChannel bufferWithChannel;
          try {
            bufferWithChannel = writeBuffer.getNextBuffer(segment);
          } catch (Throwable t) {
            outputGate.getBufferPool().recycle(segment);
            throw new FlinkRuntimeException("Shuffle write failure.", t);
          }
          Buffer buffer = bufferWithChannel.getBuffer();
          int subpartitionIndex = bufferWithChannel.getChannelIndex();
          updateStatistics(bufferWithChannel.getBuffer(), isBroadcast);
          writeCompressedBufferIfPossible(buffer, subpartitionIndex);
        }
        outputGate.regionFinish();
      } catch (InterruptedException e) {
        throw new IOException("Failed to flush the write buffer, broadcast = " + isBroadcast, e);
      }
    }
    releaseWriteBuffer(writeBuffer);
  }

  private void releaseWriteBuffer(WriteBuffer writeBuffer) {
    if (writeBuffer != null) {
      writeBuffer.release();
    }
  }

  private void updateStatistics(Buffer buffer, boolean isBroadcast) {
    numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
    long readableBytes = buffer.readableBytes() - BufferUtils.HEADER_LENGTH;
    // numBytesProduced.inc(readableBytes);
    numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
  }

  private void writeCompressedBufferIfPossible(Buffer buffer, int targetSubpartition)
      throws InterruptedException {
    Buffer compressedBuffer = null;
    try {
      if (canBeCompressed(buffer)) {
        Buffer dataBuffer =
            buffer.readOnlySlice(
                BufferUtils.HEADER_LENGTH, buffer.getSize() - BufferUtils.HEADER_LENGTH);
        compressedBuffer = checkNotNull(bufferCompressor).compressToIntermediateBuffer(dataBuffer);
      }
      BufferUtils.setCompressedDataWithHeader(buffer, compressedBuffer);
    } catch (Throwable throwable) {
      buffer.recycleBuffer();
      throw new ShuffleException("Shuffle write failure.", throwable);
    } finally {
      if (compressedBuffer != null && compressedBuffer.isCompressed()) {
        compressedBuffer.setReaderIndex(0);
        compressedBuffer.recycleBuffer();
      }
    }
    outputGate.write(buffer, targetSubpartition);
  }

  @Override
  public synchronized void close() {
    Throwable closeException = null;
    try {
      releaseWriteBuffer(unicastWriteBuffer);
    } catch (Throwable throwable) {
      closeException = throwable;
      LOG.error("Failed to release unicast sort buffer.", throwable);
    }

    try {
      releaseWriteBuffer(broadcastWriteBuffer);
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to release broadcast sort buffer.", throwable);
    }

    try {
      super.close();
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to call super#close() method.", throwable);
    }

    try {
      outputGate.close();
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to close remote shuffle output gate.", throwable);
    }

    if (closeException != null) {
      ExceptionUtils.translateToRuntimeException(closeException);
    }
  }

  @Override
  public void finish() throws IOException {
    checkState(!isReleased(), "Result partition is already released.");
    broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
    checkState(
        unicastWriteBuffer == null || unicastWriteBuffer.isReleased(),
        "The unicast sort buffer should be either null or released.");
    flushBroadcastWriteBuffer();
    try {
      outputGate.finish();
    } catch (InterruptedException e) {
      throw new IOException("Output gate fails to finish.", e);
    }
    super.finish();
  }
}
