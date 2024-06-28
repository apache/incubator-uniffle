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

package org.apache.uniffle.common.netty.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.JavaUtils;

public class FileSegmentManagedBuffer extends ManagedBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(FileSegmentManagedBuffer.class);
  private final File file;
  private final long offset;
  private final int length;

  public FileSegmentManagedBuffer(File file, long offset, int length) {
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int size() {
    return length;
  }

  @Override
  public ByteBuf byteBuf() {
    return Unpooled.wrappedBuffer(this.nioByteBuffer());
  }

  @Override
  public ByteBuffer nioByteBuffer() {
    FileChannel channel = null;
    try {
      channel = new RandomAccessFile(file, "r").getChannel();
      ByteBuffer buf = ByteBuffer.allocate(length);
      channel.position(offset);
      while (buf.remaining() != 0) {
        if (channel.read(buf) == -1) {
          throw new IOException(
              String.format(
                  "Reached EOF before filling buffer.offset=%s,file=%s,buf.remaining=%s",
                  offset, file.getAbsoluteFile(), buf.remaining()));
        }
      }
      buf.flip();
      return buf;
    } catch (IOException e) {
      String errorMessage = "Error in reading " + this;
      try {
        if (channel != null) {
          long size = channel.size();
          errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
        }
      } catch (IOException ignored) {
        // ignore
      }

      LOG.error(errorMessage, e);
      return ByteBuffer.allocate(0);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() {
    FileChannel fileChannel;
    try {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    } catch (IOException e) {
      throw new RssException("Error in reading " + file);
    }
    return new DefaultFileRegion(fileChannel, offset, length);
  }
}
