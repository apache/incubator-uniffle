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

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferPartialInputStreamImpl extends PartialInputStream {

  private ByteBuffer buffer;
  private final long start; // the start of source input stream
  private final long end; // the end of source input stream

  public BufferPartialInputStreamImpl(ByteBuffer byteBuffer, long start, long end)
      throws IOException {
    if (start < 0) {
      throw new IOException("Negative position for channel!");
    }
    this.buffer = byteBuffer;
    this.start = start;
    this.end = end;
    this.buffer.position((int) start);
  }

  @Override
  public int read() throws IOException {
    if (available() <= 0) {
      return -1;
    }
    return this.buffer.get() & 0xff;
  }

  @Override
  public int available() throws IOException {
    return (int) (end - this.buffer.position());
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getEnd() {
    return end;
  }
}
