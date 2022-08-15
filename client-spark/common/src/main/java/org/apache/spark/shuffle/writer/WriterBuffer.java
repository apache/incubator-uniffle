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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  private long copyTime = 0;
  private byte[] buffer;
  private int bufferSize;
  private int nextOffset = 0;
  private List<WrappedBuffer> buffers = Lists.newArrayList();
  private int dataLength = 0;
  private int memoryUsed = 0;

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void addRecord(byte[] recordBuffer, int length) {
    int require = calculateMemoryCost(length);
    int hasCopied = 0;
    if (require > 0) {
      if (buffer != null) {
        int toCopy = buffer.length - nextOffset;
        if (toCopy > 0) {
          hasCopied = toCopy;
          System.arraycopy(recordBuffer, 0, buffer, nextOffset, hasCopied);
        }
        buffers.add(new WrappedBuffer(buffer, buffer.length));
      }
      buffer = new byte[require];
      nextOffset = 0;
    }
    System.arraycopy(recordBuffer, hasCopied, buffer, nextOffset, length - hasCopied);
    nextOffset += length - hasCopied;
    memoryUsed += require;
    dataLength += length;
  }

  public int calculateMemoryCost(int length) {
    if (buffer == null) {
      return Math.max(length, bufferSize);
    }
    int require = length + nextOffset - buffer.length;
    if (require <= 0) {
      return 0;
    }
    return Math.max(require, bufferSize);
  }

  public byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long start = System.currentTimeMillis();
    for (WrappedBuffer wrappedBuffer : buffers) {
      System.arraycopy(wrappedBuffer.getBuffer(), 0, data, offset, wrappedBuffer.getSize());
      offset += wrappedBuffer.getSize();
    }
    // nextOffset is the length of current buffer used
    System.arraycopy(buffer, 0, data, offset, nextOffset);
    copyTime += System.currentTimeMillis() - start;
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
