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
import java.util.LinkedList;

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
import org.apache.flink.runtime.io.network.partition.SortBuffer;
import org.apache.flink.runtime.io.network.partition.SortMergeResultPartition;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.shuffle.buffer.BufferWithChannel;
import org.apache.uniffle.shuffle.buffer.SortBasedWriteBuffer;
import org.apache.uniffle.shuffle.buffer.WriteBuffer;
import org.apache.uniffle.shuffle.exception.ShuffleException;
import org.apache.uniffle.shuffle.utils.ExceptionUtils;
import org.apache.uniffle.shuffle.writer.RssShuffleOutputGate;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.uniffle.shuffle.buffer.WriteBuffer.HEADER_LENGTH;

/**
 * In the process of implementing {@link RssShuffleResultPartition}, we referenced {@link
 * SortMergeResultPartition}, and we defined a WriteBuffer similar to {@link SortBuffer}.
 *
 * <p>{@link RssShuffleResultPartition} appends records and events to {@link WriteBuffer}.
 *
 * <p>{@link WriteBuffer} is full, all data in the {@link WriteBuffer} will write out to shuffle
 * server in subpartition index order sequentially. Large records that can not be appended to an
 * empty {@link WriteBuffer} will be spilled directly.
 */
public class RssShuffleResultPartition extends ResultPartition {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleResultPartition.class);

  /**
   * Size of network buffer and write buffer. Int type value, the size of the network buffer and
   * write buffer (buffer size), set by taskmanager.memory.segment-size
   */
  private final int networkBufferSize;

  /**
   * {@link WriteBuffer} for records sent by {@link #broadcastRecord(ByteBuffer)}. Like {@link
   * SortMergeResultPartition}#broadcastSortBuffer
   */
  private WriteBuffer broadcastWriteBuffer;

  /**
   * {@link WriteBuffer} for records sent by {@link #emitRecord(ByteBuffer, int)}. Like {@link
   * SortMergeResultPartition}#unicastSortBuffer
   */
  private WriteBuffer unicastWriteBuffer;

  /** All available network buffers can be used by this result partition for a data region. */
  private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

  /** Utility to write data to uniffle shuffle servers. */
  private final RssShuffleOutputGate outputGate;

  /**
   * There is a buffer for writing shuffle-server. We declare this variable in the form of a fixed
   * variable.
   */
  private static final int NUM_REQUIRED_BUFFER = 1;

  public RssShuffleResultPartition(
      String taskName,
      int partitionIndex,
      ResultPartitionID partitionId,
      ResultPartitionType partitionType,
      int numSubpartitions,
      int numTargetKeyGroups,
      int networkBufferSize,
      ResultPartitionManager partitionManager,
      BufferCompressor bufferCompressor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      RssShuffleOutputGate outputGate) {
    super(
        taskName,
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

  /**
   * Registers a buffer pool with this result partition.
   *
   * <p>There is one pool for each result partition, which is shared by all its sub partitions.
   *
   * <p>The pool is registered with the partition *after* it as been constructed in order to conform
   * to the life-cycle of task registrations in the {@link TaskExecutor}.
   */
  @Override
  public synchronized void setup() throws IOException {
    // ResultPartition#setUp
    super.setup();

    // We try to apply for MemorySegment and block waiting.
    requestGuaranteedBuffers();

    // We initialize RssShuffleOutputGate.
    try {
      outputGate.setup();
    } catch (Throwable throwable) {
      ExceptionUtils.logAndThrowRuntimeException(
          "Failed to setup rss shuffle outputgate.", throwable);
    }

    LOG.info("Rss ShuffleResult Partition {} initialized.", getPartitionId());
  }

  private void requestGuaranteedBuffers() throws IOException {
    try {
      while (freeSegments.size() < NUM_REQUIRED_BUFFER) {
        freeSegments.add(checkNotNull(bufferPool.requestMemorySegmentBlocking()));
      }
    } catch (InterruptedException exception) {
      releaseFreeSegments();
      ExceptionUtils.logAndThrowIOException(
          "Failed to allocate buffers for result partition.", exception);
    }
  }

  // ------------------------------------------------------------------------

  /** Writes the given serialized record to the target subpartition. */
  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
  }

  private void emit(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws IOException {

    checkInProduceState();
    WriteBuffer writeBuffer = isBroadcast ? getBroadcastWriteBuffer() : getUnicastWriteBuffer();
    if (!writeBuffer.append(record, targetSubpartition, dataType)) {
      return;
    }

    try {
      if (!writeBuffer.hasRemaining()) {
        // the record can not be appended to the free sort buffer because it is too large
        writeBuffer.finish();
        writeBuffer.release();
        writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
        return;
      }
      flushWriteBuffer(writeBuffer, isBroadcast);
    } catch (InterruptedException e) {
      ExceptionUtils.logAndThrowRuntimeException("Failed to flush the write buffer.", e);
    }

    emit(record, targetSubpartition, dataType, isBroadcast);
  }

  private void writeLargeRecord(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws InterruptedException {
    outputGate.startRegion(isBroadcast);
    while (record.hasRemaining()) {
      MemorySegment writeBuffer = outputGate.getBufferPool().requestMemorySegmentBlocking();
      int toCopy = Math.min(record.remaining(), writeBuffer.size() - HEADER_LENGTH);
      writeBuffer.put(HEADER_LENGTH, record, toCopy);
      NetworkBuffer buffer =
          new NetworkBuffer(
              writeBuffer, outputGate.getBufferPool(), dataType, toCopy + HEADER_LENGTH);
      updateStatistics(buffer, isBroadcast);
      compressWriterBufferAndWriteIfPossible(buffer, targetSubpartition);
    }
    outputGate.finishRegion();
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
      throw new RuntimeException(t);
    }
  }

  @Override
  public void flush(int subpartitionIndex) {
    flushAll();
  }

  private WriteBuffer getUnicastWriteBuffer() throws IOException {
    flushBroadcastWriteBuffer();

    if (unicastWriteBuffer != null && !unicastWriteBuffer.isFinished()) {
      return unicastWriteBuffer;
    }

    unicastWriteBuffer =
        new SortBasedWriteBuffer(
            bufferPool, numSubpartitions, networkBufferSize, NUM_REQUIRED_BUFFER);
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

    broadcastWriteBuffer =
        new SortBasedWriteBuffer(
            bufferPool, numSubpartitions, networkBufferSize, NUM_REQUIRED_BUFFER);
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
        outputGate.startRegion(isBroadcast);
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
          compressWriterBufferAndWriteIfPossible(buffer, subpartitionIndex);
        }
        outputGate.finishRegion();
      } catch (InterruptedException e) {
        throw new IOException("Failed to flush the write buffer, broadcast = " + isBroadcast, e);
      }
    }
    releaseWriteBuffer(writeBuffer);
  }

  private void updateStatistics(Buffer buffer, boolean isBroadcast) {
    numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
    long readableBytes = buffer.readableBytes() - HEADER_LENGTH;
    numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
  }

  private void compressWriterBufferAndWriteIfPossible(Buffer buffer, int targetSubpartition)
      throws InterruptedException {
    Buffer compressedBuffer = null;
    try {
      if (canBeCompressed(buffer)) {
        Buffer dataBuffer = buffer.readOnlySlice(HEADER_LENGTH, buffer.getSize() - HEADER_LENGTH);
        compressedBuffer = checkNotNull(bufferCompressor).compressToIntermediateBuffer(dataBuffer);
      }
      boolean isCompressed = compressedBuffer != null && compressedBuffer.isCompressed();
      int dataLength =
          isCompressed ? compressedBuffer.readableBytes() : buffer.readableBytes() - HEADER_LENGTH;
      ByteBuf byteBuf = buffer.asByteBuf();
      WriteBuffer.setWriterBufferHeader(byteBuf, buffer.getDataType(), isCompressed, dataLength);
      if (isCompressed) {
        byteBuf.writeBytes(compressedBuffer.asByteBuf());
      }
      buffer.setSize(dataLength + HEADER_LENGTH);
      outputGate.write(buffer, targetSubpartition);
    } catch (Throwable throwable) {
      buffer.recycleBuffer();
      throw new ShuffleException("Shuffle write failure.", throwable);
    } finally {
      if (compressedBuffer != null && compressedBuffer.isCompressed()) {
        compressedBuffer.setReaderIndex(0);
        compressedBuffer.recycleBuffer();
      }
    }
  }

  private void releaseFreeSegments() {
    if (bufferPool != null) {
      freeSegments.forEach(buffer -> bufferPool.recycle(buffer));
      freeSegments.clear();
    }
  }

  private void releaseWriteBuffer(WriteBuffer writeBuffer) {
    if (writeBuffer != null) {
      writeBuffer.release();
    }
  }

  /**
   * the close method will be always called by the task thread, so we use the synchronous method to
   * close.
   */
  @Override
  public synchronized void close() {
    releaseFreeSegments();
    releaseWriteBuffer(unicastWriteBuffer);
    releaseWriteBuffer(broadcastWriteBuffer);
    outputGate.close();
    super.close();
  }

  /**
   * Finishes the result partition.
   *
   * <p>After this operation, it is not possible to add further data to the result partition.
   *
   * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
   */
  @Override
  public void finish() throws IOException {

    if (!this.isReleased()) {
      throw new IllegalStateException("Result Partition is already released!");
    }

    if (unicastWriteBuffer.isReleased() || unicastWriteBuffer == null) {
      throw new IllegalStateException("The Unicast Write Buffer Cannot be null or release!");
    }

    broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
    flushBroadcastWriteBuffer();

    try {
      outputGate.finish();
    } catch (InterruptedException e) {
      throw new IOException("RssShuffleOutputGate fails to finish.", e);
    }

    super.finish();
  }
}
