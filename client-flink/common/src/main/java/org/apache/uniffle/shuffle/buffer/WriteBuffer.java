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

package org.apache.uniffle.shuffle.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Data of different channels can be appended to a {@link WriteBuffer} and after the {@link
 * WriteBuffer} is full or finished, the appended data can be copied from it in channel index order.
 *
 * <p>The lifecycle of a {@link WriteBuffer} can be: new, write, [read, reset, write], finish, read,
 * release. There can be multiple [read, reset, write] operations before finish.
 */
public abstract class WriteBuffer {

  /** dataType(1) + isCompressed(1) + bufferSize(4) Corresponds to ${@link WriteBufferHeader}* */
  public static final int HEADER_LENGTH = 1 + 1 + 4;

  /**
   * Appends data of the specified channel to this {@link WriteBuffer} and returns true if this
   * {@link WriteBuffer} is full.
   */
  public abstract boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
      throws IOException;

  /**
   * Copies data in this {@link WriteBuffer} to the target {@link MemorySegment} in channel index
   * order and returns {@link BufferWithChannel} which contains the copied data and the
   * corresponding channel index.
   */
  public abstract BufferWithChannel getNextBuffer(@Nullable MemorySegment transitBuffer);

  /** Returns the total number of records written to this {@link WriteBuffer}. */
  public abstract long numTotalRecords();

  /** Returns the total number of bytes written to this {@link WriteBuffer}. */
  public abstract long numTotalBytes();

  /** Finishes this {@link WriteBuffer} which means no record can be appended any more. */
  public abstract void finish();

  /** Whether this {@link WriteBuffer} is finished or not. */
  public abstract boolean isFinished();

  /** Releases this {@link WriteBuffer} which releases all resources. */
  public abstract void release();

  /** Whether this {@link WriteBuffer} is released or not. */
  public abstract boolean isReleased();

  /** Resets this {@link WriteBuffer} to be reused for data appending. */
  public abstract void reset();

  /** Returns true if there is still data can be consumed in this {@link WriteBuffer}. */
  public abstract boolean hasRemaining();

  public static void setWriterBufferHeader(
      ByteBuf byteBuf, Buffer.DataType dataType, boolean isCompressed, int dataLength) {
    byteBuf.writerIndex(0);
    byteBuf.writeByte(dataType.ordinal());
    byteBuf.writeBoolean(isCompressed);
    byteBuf.writeInt(dataLength);
  }

  public static WriteBufferHeader getWriteBufferHeader(Buffer buffer, int position) {
    ByteBuf byteBuf = buffer.asByteBuf();
    byteBuf.readerIndex(position);
    return new WriteBufferHeader(
        Buffer.DataType.values()[byteBuf.readByte()], byteBuf.readBoolean(), byteBuf.readInt());
  }
}
