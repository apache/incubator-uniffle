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

package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

public class BlockFlushFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(BlockFlushFileReader.class);
  private static final int BUFFER_SIZE = 4096;

  private String dataFile;
  private FileInputStream dataInput;
  private FileChannel dataFileChannel;
  boolean stop = false;

  // blockid -> BlockInputStream
  private final Map<Long, BlockInputStream> inputStreamMap = JavaUtils.newConcurrentMap();
  private final LinkedHashMap<Long, FileBasedShuffleSegment> indexSegments = new LinkedHashMap<>();

  private FlushFileReader flushFileReader;
  private volatile Throwable readThrowable = null;
  // Even though there are many BlockInputStream, these BlockInputStream must
  // be executed in the same thread, we called the Merge Thread. When the buffer
  // of BlockInputStream have been read out, we can notify flushFileReader by
  // unlock. Then flushFileReader will load the buffer, and Merge will read the
  // buffer of BlockInputStream until flushFileReader load done and unlock.
  private final ReentrantLock lock = new ReentrantLock(true);

  private final int ringBufferSize;
  private final int mask;

  public BlockFlushFileReader(String dataFile, String indexFile, int ringBufferSize)
      throws IOException {
    // Make sure flush file will not be updated
    this.ringBufferSize = ringBufferSize;
    this.mask = ringBufferSize - 1;
    loadShuffleIndex(indexFile);
    this.dataFile = dataFile;
    this.dataInput = new FileInputStream(dataFile);
    this.dataFileChannel = dataInput.getChannel();
    // Avoid flushFileReader noop loop
    this.lock.lock();
    this.flushFileReader = new FlushFileReader();
    this.flushFileReader.start();
  }

  public void loadShuffleIndex(String indexFileName) {
    File indexFile = new File(indexFileName);
    long indexFileSize = indexFile.length();
    int indexNum = (int) (indexFileSize / FileBasedShuffleSegment.SEGMENT_SIZE);
    int len = indexNum * FileBasedShuffleSegment.SEGMENT_SIZE;
    ByteBuffer indexData = new FileSegmentManagedBuffer(indexFile, 0, len).nioByteBuffer();
    while (indexData.hasRemaining()) {
      long offset = indexData.getLong();
      int length = indexData.getInt();
      int uncompressLength = indexData.getInt();
      long crc = indexData.getLong();
      long blockId = indexData.getLong();
      long taskAttemptId = indexData.getLong();
      FileBasedShuffleSegment fileBasedShuffleSegment =
          new FileBasedShuffleSegment(
              blockId, offset, length, uncompressLength, crc, taskAttemptId);
      indexSegments.put(fileBasedShuffleSegment.getBlockId(), fileBasedShuffleSegment);
    }
  }

  public void close() throws IOException, InterruptedException {
    if (!this.stop) {
      stop = true;
      flushFileReader.interrupt();
      flushFileReader = null;
    }
    if (dataInput != null) {
      this.dataInput.close();
      this.dataInput = null;
      this.dataFile = null;
    }
  }

  public BlockInputStream registerBlockInputStream(long blockId) {
    if (!indexSegments.containsKey(blockId)) {
      return null;
    }
    if (!inputStreamMap.containsKey(blockId)) {
      inputStreamMap.put(
          blockId, new BlockInputStream(blockId, this.indexSegments.get(blockId).getLength()));
    }
    return inputStreamMap.get(blockId);
  }

  class FlushFileReader extends Thread {
    @Override
    public void run() {
      while (!stop) {
        int available = 0;
        int process = 0;
        try {
          lock.lockInterruptibly();
          try {
            Iterator<Map.Entry<Long, FileBasedShuffleSegment>> iterator =
                indexSegments.entrySet().iterator();
            while (iterator.hasNext()) {
              FileBasedShuffleSegment segment = iterator.next().getValue();
              long blockId = segment.getBlockId();
              BlockInputStream inputStream = inputStreamMap.get(blockId);
              if (inputStream == null || inputStream.eof) {
                continue;
              }
              available++;
              if (inputStream.isBufferFull()) {
                continue;
              }
              process++;
              long off = segment.getOffset() + inputStream.getOffsetInThisBlock();
              if (dataFileChannel.position() != off) {
                dataFileChannel.position(off);
              }
              inputStream.writeBuffer();
            }
          } catch (Throwable throwable) {
            readThrowable = throwable;
            LOG.info("FlushFileReader read failed, caused by ", throwable);
            stop = true;
          } finally {
            lock.unlock();
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "statistics: load buffer available is {}, process is {}", available, process);
            }
          }
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("FlushFileReader for {} have been interrupted.", dataFile);
          }
        }
      }
    }
  }

  class Buffer {

    private byte[] bytes = new byte[BUFFER_SIZE];
    private int cap = BUFFER_SIZE;
    private int pos = cap;

    public int get() {
      return this.bytes[pos++] & 0xFF;
    }

    public int get(byte[] bs, int off, int len) {
      int r = Math.min(cap - pos, len);
      System.arraycopy(bytes, pos, bs, off, r);
      pos += r;
      return r;
    }

    public boolean readable() {
      return pos < cap;
    }

    public void writeBuffer(int length) throws IOException {
      dataFileChannel.read(ByteBuffer.wrap(this.bytes, 0, length));
      this.pos = 0;
      this.cap = length;
    }
  }

  class RingBuffer {

    Buffer[] buffers;
    // The max of int is 2147483647, the maximum bocksize supported by RingBuffer is 7.999 TB,
    // the block can't be that big. so readIndex and writeIndex cannot overflow, there's no
    // modulo operator for readIndex and writeIndex.
    int readIndex = 0;
    int writeIndex = 0;

    RingBuffer() {
      this.buffers = new Buffer[ringBufferSize];
      for (int i = 0; i < ringBufferSize; i++) {
        this.buffers[i] = new Buffer();
      }
    }

    boolean full() {
      return (writeIndex - readIndex) == ringBufferSize;
    }

    boolean empty() {
      return writeIndex == readIndex;
    }

    int write(int available) throws IOException {
      int left = available;
      while (!full() && left > 0) {
        int size = Math.min(available, BUFFER_SIZE);
        this.buffers[writeIndex & mask].writeBuffer(size);
        left -= size;
        writeIndex++;
      }
      return available - left;
    }

    int read() {
      int ret = this.buffers[readIndex & mask].get();
      if (!this.buffers[readIndex & mask].readable()) {
        readIndex++;
      }
      return ret;
    }

    int read(byte[] bs, int off, int len) {
      int total = 0;
      int end = off + len;
      while (off < end && !this.empty()) {
        Buffer buffer = this.buffers[readIndex & mask];
        int r = buffer.get(bs, off, len);
        if (!this.buffers[readIndex & mask].readable()) {
          readIndex++;
        }
        off += r;
        len -= r;
        total += r;
      }
      return total;
    }
  }

  public class BlockInputStream extends PartialInputStream {

    private long blockId;
    private RingBuffer ringBuffer;
    private boolean eof = false;
    private final int length;
    private int pos = 0;
    private int offsetInThisBlock = 0;

    public BlockInputStream(long blockId, int length) {
      this.blockId = blockId;
      this.length = length;
      this.ringBuffer = new RingBuffer();
    }

    @Override
    public int available() throws IOException {
      return length - pos;
    }

    @Override
    public long getStart() {
      return 0;
    }

    @Override
    public long getEnd() {
      return length;
    }

    public long getOffsetInThisBlock() {
      return this.offsetInThisBlock;
    }

    @Override
    public void close() throws IOException {
      try {
        inputStreamMap.remove(blockId);
        indexSegments.remove(blockId);
        if (inputStreamMap.size() == 0) {
          BlockFlushFileReader.this.close();
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    public boolean isBufferFull() {
      return ringBuffer.full();
    }

    public void writeBuffer() throws IOException {
      int size = this.ringBuffer.write(length - offsetInThisBlock);
      this.offsetInThisBlock += size;
    }

    public int read(byte[] bs, int off, int len) throws IOException {
      if (readThrowable != null) {
        throw new IOException("Read flush file failed!", readThrowable);
      }
      if (bs == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > bs.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (eof) {
        return -1;
      }
      while (ringBuffer.empty()) {
        if (lock.isHeldByCurrentThread()) {
          lock.unlock();
        }
        try {
          lock.lockInterruptibly();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      int c = this.ringBuffer.read(bs, off, len);
      pos += c;
      if (pos >= length) {
        eof = true;
      }
      return c;
    }

    @Override
    public int read() throws IOException {
      if (readThrowable != null) {
        throw new IOException("Read flush file failed!", readThrowable);
      }
      if (eof) {
        return -1;
      }
      while (ringBuffer.empty()) {
        if (lock.isHeldByCurrentThread()) {
          lock.unlock();
        }
        try {
          lock.lockInterruptibly();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      int c = this.ringBuffer.read();
      pos++;
      if (pos >= length) {
        eof = true;
      }
      return c;
    }
  }
}
