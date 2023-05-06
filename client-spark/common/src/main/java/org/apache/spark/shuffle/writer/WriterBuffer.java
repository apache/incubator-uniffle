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

package org.apache.spark.shuffle.writer;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  private long copyTime = 0;
  private ByteBuf buffer;
  private int bufferSize;
  private int bufferUsedSize = 0;
  private CompositeByteBuf compositeByteBuf;
  private int dataLength = 0;
  private int memoryUsed = 0;

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
    compositeByteBuf = Unpooled.compositeBuffer();
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      // buffer has data already, add buffer to list
      if (bufferUsedSize > 0) {
        compositeByteBuf.addComponent(true, buffer);
        bufferUsedSize = 0;
      }
      int newBufferSize = Math.max(length, bufferSize);
      buffer = Unpooled.buffer(newBufferSize);
      memoryUsed += newBufferSize;
    }
    buffer.writeBytes(Unpooled.wrappedBuffer(recordBuffer, 0, length));
    bufferUsedSize += length;
    dataLength += length;
  }

  public boolean askForMemory(long length) {
    return buffer == null || bufferUsedSize + length > bufferSize;
  }

  public ByteBuffer getData() {
    final long start = System.currentTimeMillis();
    compositeByteBuf.addComponent(true, buffer);
    final ByteBuffer data = compositeByteBuf.nioBuffer();
    copyTime += System.currentTimeMillis() - start;
    buffer.clear();
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public int getMemoryUsed() {
    return memoryUsed;
  }
}
