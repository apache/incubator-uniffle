/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.storage.handler.impl;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.storage.api.FileReader;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

public class HdfsFileReader implements FileReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);
  private Path path;
  private Configuration hadoopConf;
  private FSDataInputStream fsDataInputStream;

  public HdfsFileReader(Path path, Configuration hadoopConf) throws IOException, IllegalStateException {
    this.path = path;
    this.hadoopConf = hadoopConf;
    createStream();
  }

  private void createStream() throws IOException, IllegalStateException {
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);

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
      LOG.warn("Can't read data for path:" + path + " with offset["
          + offset + "], length[" + length + "]", e);
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

  public long getOffset() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataInputStream != null) {
      fsDataInputStream.close();
    }
  }

  public Path getPath() {
    return path;
  }
}
