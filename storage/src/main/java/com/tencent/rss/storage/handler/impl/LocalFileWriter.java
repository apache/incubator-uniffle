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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.tencent.rss.storage.common.FileBasedShuffleSegment;

public class LocalFileWriter implements Closeable {

  private DataOutputStream dataOutputStream;
  private FileOutputStream fileOutputStream;
  private long initSize;
  private long nextOffset;

  public LocalFileWriter(File file) throws IOException {
    fileOutputStream = new FileOutputStream(file, true);
    // init fsDataOutputStream
    dataOutputStream = new DataOutputStream(fileOutputStream);
    initSize = file.length();
    nextOffset = initSize;
  }

  public void writeData(byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      dataOutputStream.write(data);
      nextOffset = initSize + dataOutputStream.size();
    }
  }

  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    dataOutputStream.writeLong(segment.getOffset());
    dataOutputStream.writeInt(segment.getLength());
    dataOutputStream.writeInt(segment.getUncompressLength());
    dataOutputStream.writeLong(segment.getCrc());
    dataOutputStream.writeLong(segment.getBlockId());
    dataOutputStream.writeLong(segment.getTaskAttemptId());
  }

  public long nextOffset() {
    return nextOffset;
  }

  @Override
  public synchronized void close() throws IOException {
    if (dataOutputStream != null) {
      dataOutputStream.close();
    }
  }
}
