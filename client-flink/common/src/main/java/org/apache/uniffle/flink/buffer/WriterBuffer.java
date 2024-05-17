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

package org.apache.uniffle.flink.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Data of different channels can be appended to a {@link WriterBuffer} and after the {@link
 * WriterBuffer} is full or finished, the appended data can be copied from it in channel index
 * order.
 *
 * <p>The lifecycle of a {@link WriterBuffer} can be: new, write, [read, reset, write], finish,
 * read, release. There can be multiple [read, reset, write] operations before finish.
 */
public interface WriterBuffer {

  /** dataType(1) + isCompressed(1) + bufferSize(4) Corresponds to ${@link WriteBufferHeader}* */
  public static final int HEADER_LENGTH = 1 + 1 + 4;

  /**
   * Appends data of the specified channel to this {@link WriterBuffer} and returns true if this
   * {@link WriterBuffer} is full.
   */
  boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType) throws IOException;

  /**
   * Copies data in this {@link WriterBuffer} to the target {@link MemorySegment} in channel index
   * order and returns {@link BufferWithChannel} which contains the copied data and the
   * corresponding channel index.
   */
  BufferWithChannel getNextBuffer(@Nullable MemorySegment transitBuffer);

  /** Returns the total number of records written to this {@link WriterBuffer}. */
  long numTotalRecords();

  /** Returns the total number of bytes written to this {@link WriterBuffer}. */
  long numTotalBytes();

  /** Returns true if not all data appended to this {@link WriterBuffer} is consumed. */
  boolean hasRemaining();

  /** Finishes this {@link WriterBuffer} which means no record can be appended any more. */
  void finish();

  /** Whether this {@link WriterBuffer} is finished or not. */
  boolean isFinished();

  /** Releases this {@link WriterBuffer} which releases all resources. */
  void release();

  /** Whether this {@link WriterBuffer} is released or not. */
  boolean isReleased();

  static void setWriterBufferHeader(
      ByteBuf byteBuf, Buffer.DataType dataType, boolean isCompressed, int dataLength) {
    byteBuf.writerIndex(0);
    byteBuf.writeByte(dataType.ordinal());
    byteBuf.writeBoolean(isCompressed);
    byteBuf.writeInt(dataLength);
  }

  static WriteBufferHeader getWriteBufferHeader(Buffer buffer, int position) {
    ByteBuf byteBuf = buffer.asByteBuf();
    byteBuf.readerIndex(position);
    return new WriteBufferHeader(
        Buffer.DataType.values()[byteBuf.readByte()], byteBuf.readBoolean(), byteBuf.readInt());
  }
}
