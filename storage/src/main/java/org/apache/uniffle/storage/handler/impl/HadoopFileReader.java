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

package org.apache.uniffle.storage.handler.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.storage.api.FileReader;

public class HadoopFileReader implements FileReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileReader.class);
  private Path path;
  private Configuration hadoopConf;
  private FSDataInputStream fsDataInputStream;
  private FileSystem fileSystem;

  public HadoopFileReader(Path path, Configuration hadoopConf) throws Exception {
    this.path = path;
    this.hadoopConf = hadoopConf;
    createStream();
  }

  private void createStream() throws Exception {
    fileSystem = HadoopFilesystemProvider.getFilesystem(path, hadoopConf);

    if (!fileSystem.isFile(path)) {
      String msg = path + " don't exist or is not a file.";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    fsDataInputStream = fileSystem.open(path);
  }

  public byte[] read(long offset, int length) {
    try {
      fsDataInputStream.seek(offset);
      byte[] buf = new byte[length];
      fsDataInputStream.readFully(buf);
      return buf;
    } catch (Exception e) {
      LOG.warn(
          "Can't read data for path:"
              + path
              + " with offset["
              + offset
              + "], length["
              + length
              + "]",
          e);
    }
    return new byte[0];
  }

  public byte[] read() {
    try {
      return IOUtils.toByteArray(fsDataInputStream);
    } catch (IOException e) {
      LOG.error("Fail to read all data from {}", path, e);
      return new byte[0];
    }
  }

  @Override
  public ByteBuffer readAsByteBuffer(long offset, int length) {
    try {
      fsDataInputStream.seek(offset);
      ByteBuffer buffer = ByteBuffer.allocateDirect(length);
      readFully(buffer);
      buffer.flip();
      return buffer;
    } catch (Exception e) {
      LOG.warn(
          "Can't read buffer data for path:"
              + path
              + " with offset["
              + offset
              + "], length["
              + length
              + "]",
          e);
      return ByteBuffer.allocateDirect(0);
    }
  }

  @Override
  public ByteBuffer readAsByteBuffer() {
    try {
      long length = getFileLen();
      if (length - fsDataInputStream.getPos() > Integer.MAX_VALUE) {
        LOG.warn("File " + path + "length is too long");
        return ByteBuffer.allocateDirect(0);
      }
      return readAsByteBuffer(fsDataInputStream.getPos(), (int) length);
    } catch (Exception e) {
      LOG.warn("Can't read buffer data for path:" + path, e);
      return ByteBuffer.allocateDirect(0);
    }
  }

  public long getOffset() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataInputStream != null) {
      fsDataInputStream.close();
    }
  }

  private void readFully(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      int result = fsDataInputStream.read(buffer);
      if (result < 0) {
        return;
      }
    }
  }

  public Path getPath() {
    return path;
  }

  public long getFileLen() throws IOException {
    return fileSystem.getFileStatus(path).getLen();
  }
}
