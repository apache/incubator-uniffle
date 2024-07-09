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

package org.apache.uniffle.common.serializer;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

/*
 * PartialInputStream is a configurable partial input stream, which
 * only allows reading from start to end of the source input stream.
 * */
public class PartialInputStreamImpl extends PartialInputStream {

  private final SeekableByteChannel ch; // the source input channel
  private final long start; // the start of source input stream
  private final long end; // the end of source input stream
  private long pos; // the read offset

  private ByteBuffer bb = null;
  private byte[] bs = null;
  private byte[] b1;
  private Closeable closeable;

  public PartialInputStreamImpl(SeekableByteChannel ch, long start, long end, Closeable closeable)
      throws IOException {
    if (start < 0) {
      throw new IOException("Negative position for channel!");
    }
    this.ch = ch;
    this.start = start;
    this.end = end;
    this.closeable = closeable;
    this.pos = start;
    ch.position(start);
  }

  private int read(ByteBuffer bb) throws IOException {
    return ch.read(bb);
  }

  @Override
  public synchronized int read() throws IOException {
    if (b1 == null) {
      b1 = new byte[1];
    }
    int n = read(b1);
    if (n == 1) {
      return b1[0] & 0xff;
    }
    return -1;
  }

  @Override
  public synchronized int read(byte[] bs, int off, int len) throws IOException {
    if ((off < 0)
        || (off > bs.length)
        || (len < 0)
        || ((off + len) > bs.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    ByteBuffer bb = (this.bs == bs) ? this.bb : ByteBuffer.wrap(bs);
    bb.limit(Math.min(off + len, bb.capacity()));
    bb.position(off);
    this.bb = bb;
    this.bs = bs;
    int ret = read(bb);
    if (ret >= 0) {
      pos += ret;
    }
    return ret;
  }

  @Override
  public int available() throws IOException {
    return (int) (end - pos);
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getEnd() {
    return end;
  }

  @Override
  public void close() throws IOException {
    if (closeable != null) {
      closeable.close();
    }
  }

  private static PartialInputStreamImpl newInputStream(
      SeekableByteChannel ch, long start, long end, Closeable closeable) throws IOException {
    if (ch == null) {
      throw new NullPointerException("channel is null!");
    }
    return new PartialInputStreamImpl(ch, start, end, closeable);
  }

  public static PartialInputStreamImpl newInputStream(File file, long start, long end)
      throws IOException {
    FileInputStream input = new FileInputStream(file);
    FileChannel fc = input.getChannel();
    long size = fc.size();
    return newInputStream(
        fc,
        start,
        Math.min(end, size),
        () -> {
          input.close();
        });
  }

  public static PartialInputStreamImpl newInputStream(byte[] bytes, long start, long end)
      throws IOException {
    SeekableInMemoryByteChannel ch = new SeekableInMemoryByteChannel(bytes);
    int size = bytes.length;
    return newInputStream(ch, start, Math.min(end, size), () -> ch.close());
  }
}
