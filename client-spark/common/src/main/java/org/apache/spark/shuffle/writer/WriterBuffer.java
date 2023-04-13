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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  private AtomicLong copyTime = new AtomicLong(0);
  private AtomicReference<byte[]> buffer = new AtomicReference<>();
  private final AtomicInteger bufferSize;
  private AtomicInteger nextOffset = new AtomicInteger(0);
  private List<WrappedBuffer> buffers = Lists.newArrayList();
  private AtomicInteger dataLength = new AtomicInteger();
  private AtomicInteger memoryUsed = new AtomicInteger();

  public WriterBuffer(int bufferSize) {
    this.bufferSize = new AtomicInteger(bufferSize);
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      // buffer has data already, add buffer to list
      if (nextOffset.get() > 0) {
        buffers.add(new WrappedBuffer(buffer.get(), nextOffset.get()));
        nextOffset.set(0);
      }
      int newBufferSize = Math.max(length, bufferSize.get());
      buffer.set(new byte[newBufferSize]);
      memoryUsed.addAndGet(newBufferSize);
    }

    try {
      System.arraycopy(recordBuffer, 0, buffer.get(), nextOffset.get(), length);
    } catch (Exception e) {
      LOG.error("Unexpected exception for System.arraycopy, length[" + length + "], nextOffset["
          + nextOffset + "], bufferSize[" + bufferSize + "]");
      throw e;
    }

    nextOffset.addAndGet(length);
    dataLength.addAndGet(length);
  }

  public boolean askForMemory(long length) {
    return buffer.get() == null || nextOffset.get() + length > bufferSize.get();
  }

  public byte[] getData() {
    byte[] data = new byte[dataLength.get()];
    int offset = 0;
    long start = System.currentTimeMillis();
    for (WrappedBuffer wrappedBuffer : buffers) {
      System.arraycopy(wrappedBuffer.getBuffer(), 0, data, offset, wrappedBuffer.getSize());
      offset += wrappedBuffer.getSize();
    }
    // nextOffset is the length of current buffer used
    System.arraycopy(buffer.get(), 0, data, offset, nextOffset.get());
    copyTime.addAndGet(System.currentTimeMillis() - start);
    return data;
  }

  public int getDataLength() {
    return dataLength.get();
  }

  public long getCopyTime() {
    return copyTime.get();
  }

  public int getMemoryUsed() {
    return memoryUsed.get();
  }

  private static final class WrappedBuffer {

    byte[] buffer;
    int size;

    WrappedBuffer(byte[] buffer, int size) {
      this.buffer = buffer;
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }
}
