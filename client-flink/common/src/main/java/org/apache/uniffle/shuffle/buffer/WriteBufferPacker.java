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

import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.shuffle.exception.BiConsumerWithException;

import static org.apache.uniffle.shuffle.buffer.WriterBuffer.HEADER_LENGTH;
import static org.apache.uniffle.shuffle.utils.CommonUtils.checkState;

public class WriteBufferPacker {
  private static Logger logger = LoggerFactory.getLogger(WriteBufferPacker.class);

  private final BiConsumerWithException<ByteBuf, Integer, InterruptedException> ripeBufferHandler;

  private Buffer cachedBuffer;

  private int currentSubIdx = -1;

  public WriteBufferPacker(
      BiConsumerWithException<ByteBuf, Integer, InterruptedException> ripeBufferHandler) {
    this.ripeBufferHandler = ripeBufferHandler;
  }

  public void process(Buffer buffer, int subIdx) throws InterruptedException {
    if (buffer == null) {
      return;
    }

    if (buffer.readableBytes() == 0) {
      buffer.recycleBuffer();
      return;
    }

    if (cachedBuffer == null) {
      cachedBuffer = buffer;
      currentSubIdx = subIdx;
    } else if (currentSubIdx != subIdx) {
      Buffer dumpedBuffer = cachedBuffer;
      cachedBuffer = buffer;
      int targetSubIdx = currentSubIdx;
      currentSubIdx = subIdx;
      handleRipeBuffer(dumpedBuffer, targetSubIdx);
    } else {
      if (cachedBuffer.readableBytes() + buffer.readableBytes() <= cachedBuffer.getMaxCapacity()) {
        cachedBuffer.asByteBuf().writeBytes(buffer.asByteBuf());
        buffer.recycleBuffer();
      } else {
        Buffer dumpedBuffer = cachedBuffer;
        cachedBuffer = buffer;
        handleRipeBuffer(dumpedBuffer, currentSubIdx);
      }
    }
  }

  public void drain() throws InterruptedException {
    if (cachedBuffer != null) {
      Buffer dumpedBuffer = cachedBuffer;
      cachedBuffer = null;
      handleRipeBuffer(dumpedBuffer, currentSubIdx);
    }
    currentSubIdx = -1;
  }

  private void handleRipeBuffer(Buffer buffer, int subIdx) throws InterruptedException {
    buffer.setCompressed(false);
    ripeBufferHandler.accept(buffer.asByteBuf(), subIdx);
  }

  public static Queue<Buffer> unpack(ByteBuf byteBuf) {
    Queue<Buffer> buffers = new ArrayDeque<>();
    try {
      checkState(byteBuf instanceof Buffer, "Illegal buffer type.");

      Buffer buffer = (Buffer) byteBuf;
      int position = 0;
      int totalBytes = buffer.readableBytes();
      while (position < totalBytes) {
        WriteBufferHeader bufferHeader = WriterBuffer.getWriteBufferHeader(buffer, position);
        position += HEADER_LENGTH;

        Buffer slice = buffer.readOnlySlice(position, bufferHeader.getSize());
        position += bufferHeader.getSize();

        buffers.add(
            new UnpackSlicedBuffer(
                slice,
                bufferHeader.getDataType(),
                bufferHeader.isCompressed(),
                bufferHeader.getSize()));
        slice.retainBuffer();
      }
      return buffers;
    } catch (Throwable throwable) {
      buffers.forEach(Buffer::recycleBuffer);
      throw throwable;
    } finally {
      byteBuf.release();
    }
  }

  public void close() {
    if (cachedBuffer != null) {
      cachedBuffer.recycleBuffer();
      cachedBuffer = null;
    }
    currentSubIdx = -1;
  }
}
