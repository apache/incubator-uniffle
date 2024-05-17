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

package org.apache.uniffle.flink.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.SortMergeResultPartition;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.flink.buffer.BufferWithChannel;
import org.apache.uniffle.flink.buffer.SortBasedWriterBuffer;
import org.apache.uniffle.flink.buffer.WriterBuffer;
import org.apache.uniffle.flink.exception.ShuffleException;
import org.apache.uniffle.flink.utils.ShuffleUtils;
import org.apache.uniffle.flink.writer.RssShuffleOutputGateCommon;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.uniffle.flink.buffer.WriterBuffer.HEADER_LENGTH;

public class RssShuffleResultPartitionCommon {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleResultPartitionCommon.class);

  /**
   * Size of network buffer and write buffer. Int type value, the size of the network buffer and
   * write buffer (buffer size), set by taskmanager.memory.segment-size
   */
  private final int networkBufferSize;

  /**
   * {@link WriterBuffer} for records sent by {@link #broadcastRecord(ByteBuffer)}. Like {@link
   * SortMergeResultPartition}#broadcastSortBuffer
   */
  private WriterBuffer broadcastWriteBuffer;

  private WriterBuffer unicastWriteBuffer;

  /** All available network buffers can be used by this result partition for a data region. */
  private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

  /** Utility to write data to rss shuffle servers. */
  private final RssShuffleOutputGateCommon outputGate;

  /**
   * There is a buffer for writing shuffle-server. We declare this variable in the form of a fixed
   * variable.
   */
  private static final int NUM_REQUIRED_BUFFER = 1;

  private Runnable checkProducerState;
  private BufferPool bufferPool;

  private int numSubpartitions;

  private BufferCompressor bufferCompressor;
  private Function<Buffer, Boolean> canBeCompressed;

  private final BiConsumer<Buffer, Boolean> statisticsConsumer;

  public RssShuffleResultPartitionCommon(
      int networkBufferSize,
      RssShuffleOutputGateCommon outputGate,
      BiConsumer<Buffer, Boolean> statisticsConsumer,
      int numSubpartitions) {
    this.outputGate = outputGate;
    this.networkBufferSize = networkBufferSize;
    this.statisticsConsumer = statisticsConsumer;
    this.numSubpartitions = numSubpartitions;
  }

  public synchronized void setup(
      BufferPool bufferPool,
      BufferCompressor bufferCompressor,
      Function<Buffer, Boolean> canBeCompressed,
      Runnable checkProduceState)
      throws IOException {
    LOG.info("Setup {}", this);
    this.bufferPool = bufferPool;
    this.bufferCompressor = bufferCompressor;
    this.canBeCompressed = canBeCompressed;
    this.checkProducerState = checkProduceState;
    try {
      outputGate.setup();
    } catch (Throwable throwable) {
      LOG.error("Failed to setup remote output gate.", throwable);
    }
  }

  // ------------------------------------------------------------------------

  public void emit(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws IOException {

    checkProducerState.run();
    WriterBuffer writeBuffer = isBroadcast ? getBroadcastWriteBuffer() : getUnicastWriteBuffer();
    if (!writeBuffer.append(record, targetSubpartition, dataType)) {
      return;
    }

    try {
      if (!writeBuffer.hasRemaining()) {
        // the record can not be appended to the free sort buffer because it is too large
        writeBuffer.release();
        writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
        return;
      }
      flushWriteBuffer(writeBuffer, isBroadcast);
    } catch (InterruptedException e) {
      ShuffleUtils.logAndThrowRuntimeException("Failed to flush the write buffer.", e);
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
      statisticsConsumer.accept(buffer, isBroadcast);
      compressWriterBufferAndWriteIfPossible(buffer, targetSubpartition);
    }
    outputGate.finishRegion();
  }

  public void broadcastRecord(ByteBuffer record) throws IOException {
    broadcast(record, Buffer.DataType.DATA_BUFFER);
  }

  public void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
    emit(record, 0, dataType, true);
  }

  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
    try {
      ByteBuffer serializedEvent = buffer.getNioBufferReadable();
      broadcast(serializedEvent, buffer.getDataType());
    } finally {
      buffer.recycleBuffer();
    }
  }

  public void flushAll() {
    try {
      flushUnicastWriteBuffer();
      flushBroadcastWriteBuffer();
    } catch (Throwable t) {
      LOG.error("Failed to flush the current sort buffer.", t);
      throw new RuntimeException(t);
    }
  }

  private WriterBuffer getUnicastWriteBuffer() throws IOException {
    flushBroadcastWriteBuffer();

    if (unicastWriteBuffer != null && !unicastWriteBuffer.isFinished()) {
      return unicastWriteBuffer;
    }

    unicastWriteBuffer =
        new SortBasedWriterBuffer(
            bufferPool, numSubpartitions, networkBufferSize, NUM_REQUIRED_BUFFER);
    return unicastWriteBuffer;
  }

  private void flushBroadcastWriteBuffer() throws IOException {
    flushWriteBuffer(broadcastWriteBuffer, true);
  }

  private WriterBuffer getBroadcastWriteBuffer() throws IOException {
    flushUnicastWriteBuffer();

    if (broadcastWriteBuffer != null && !broadcastWriteBuffer.isFinished()) {
      return broadcastWriteBuffer;
    }

    broadcastWriteBuffer =
        new SortBasedWriterBuffer(
            bufferPool, numSubpartitions, networkBufferSize, NUM_REQUIRED_BUFFER);
    return broadcastWriteBuffer;
  }

  private void flushUnicastWriteBuffer() throws IOException {
    flushWriteBuffer(unicastWriteBuffer, false);
  }

  private void flushWriteBuffer(WriterBuffer writeBuffer, boolean isBroadcast) throws IOException {
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
          statisticsConsumer.accept(bufferWithChannel.getBuffer(), isBroadcast);
          compressWriterBufferAndWriteIfPossible(buffer, subpartitionIndex);
        }
        outputGate.finishRegion();
      } catch (InterruptedException e) {
        throw new IOException("Failed to flush the write buffer, broadcast = " + isBroadcast, e);
      }
    }
    releaseWriteBuffer(writeBuffer);
  }

  private void compressWriterBufferAndWriteIfPossible(Buffer buffer, int targetSubpartition)
      throws InterruptedException {
    Buffer compressedBuffer = null;
    try {
      if (canBeCompressed.apply(buffer)) {
        Buffer dataBuffer = buffer.readOnlySlice(HEADER_LENGTH, buffer.getSize() - HEADER_LENGTH);
        compressedBuffer = checkNotNull(bufferCompressor).compressToIntermediateBuffer(dataBuffer);
      }
      boolean isCompressed = compressedBuffer != null && compressedBuffer.isCompressed();
      int dataLength =
          isCompressed ? compressedBuffer.readableBytes() : buffer.readableBytes() - HEADER_LENGTH;
      ByteBuf byteBuf = buffer.asByteBuf();
      WriterBuffer.setWriterBufferHeader(byteBuf, buffer.getDataType(), isCompressed, dataLength);
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

  private void releaseWriteBuffer(WriterBuffer writeBuffer) {
    if (writeBuffer != null) {
      writeBuffer.release();
    }
  }

  public synchronized void close(Runnable closeHandler) {
    releaseFreeSegments();
    releaseWriteBuffer(unicastWriteBuffer);
    releaseWriteBuffer(broadcastWriteBuffer);
    closeHandler.run();
    outputGate.close();
  }

  public void finish() throws IOException {

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
  }
}
