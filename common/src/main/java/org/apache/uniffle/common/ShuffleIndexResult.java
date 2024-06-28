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

package org.apache.uniffle.common;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleIndexResult {
  private final ManagedBuffer buffer;
  private long dataFileLen;

  public ShuffleIndexResult() {
    this(ByteBuffer.wrap(new byte[0]), -1);
  }

  public ShuffleIndexResult(byte[] data, long dataFileLen) {
    this(data != null ? ByteBuffer.wrap(data) : null, dataFileLen);
  }

  public ShuffleIndexResult(ByteBuffer data, long dataFileLen) {
    this.buffer =
        new NettyManagedBuffer(data != null ? Unpooled.wrappedBuffer(data) : Unpooled.EMPTY_BUFFER);
    this.dataFileLen = dataFileLen;
  }

  public ShuffleIndexResult(ManagedBuffer buffer, long dataFileLen) {
    this.buffer = buffer;
    this.dataFileLen = dataFileLen;
  }

  public byte[] getData() {
    if (buffer == null) {
      return null;
    }
    if (buffer.nioByteBuffer().hasArray()) {
      return buffer.nioByteBuffer().array();
    }
    return ByteBufUtils.readBytes(buffer.byteBuf());
  }

  public ByteBuffer getIndexData() {
    return buffer.nioByteBuffer();
  }

  public long getDataFileLen() {
    return dataFileLen;
  }

  public boolean isEmpty() {
    return buffer == null || buffer.size() == 0;
  }

  public void release() {
    if (this.buffer != null) {
      this.buffer.release();
    }
  }

  public ManagedBuffer getManagedBuffer() {
    return buffer;
  }
}
