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

import java.nio.ByteBuffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

public class UnpackSlicedBuffer implements Buffer {
  private final Buffer buffer;

  private DataType dataType;

  private boolean isCompressed;

  private final int size;

  UnpackSlicedBuffer(Buffer buffer, DataType dataType, boolean isCompressed, int size) {
    this.buffer = buffer;
    this.dataType = dataType;
    this.isCompressed = isCompressed;
    this.size = size;
  }

  @Override
  public boolean isBuffer() {
    return dataType.isBuffer();
  }

  @Override
  public MemorySegment getMemorySegment() {
    return buffer.getMemorySegment();
  }

  @Override
  public int getMemorySegmentOffset() {
    return buffer.getMemorySegmentOffset();
  }

  @Override
  public BufferRecycler getRecycler() {
    return buffer.getRecycler();
  }

  @Override
  public void recycleBuffer() {
    buffer.recycleBuffer();
  }

  @Override
  public boolean isRecycled() {
    return buffer.isRecycled();
  }

  @Override
  public Buffer retainBuffer() {
    return buffer.retainBuffer();
  }

  @Override
  public Buffer readOnlySlice() {
    return buffer.readOnlySlice();
  }

  @Override
  public Buffer readOnlySlice(int i, int i1) {
    return buffer.readOnlySlice(i, i1);
  }

  @Override
  public int getMaxCapacity() {
    return buffer.getMaxCapacity();
  }

  @Override
  public int getReaderIndex() {
    return buffer.getReaderIndex();
  }

  @Override
  public void setReaderIndex(int i) throws IndexOutOfBoundsException {
    buffer.setReaderIndex(i);
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void setSize(int i) {
    buffer.setSize(i);
  }

  @Override
  public int readableBytes() {
    return buffer.readableBytes();
  }

  @Override
  public ByteBuffer getNioBufferReadable() {
    return buffer.getNioBufferReadable();
  }

  @Override
  public ByteBuffer getNioBuffer(int i, int i1) throws IndexOutOfBoundsException {
    return buffer.getNioBuffer(i, i1);
  }

  @Override
  public void setAllocator(ByteBufAllocator byteBufAllocator) {
    buffer.setAllocator(byteBufAllocator);
  }

  @Override
  public ByteBuf asByteBuf() {
    return buffer.asByteBuf();
  }

  @Override
  public boolean isCompressed() {
    return isCompressed;
  }

  @Override
  public void setCompressed(boolean b) {
    isCompressed = b;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public int refCnt() {
    return buffer.refCnt();
  }
}
