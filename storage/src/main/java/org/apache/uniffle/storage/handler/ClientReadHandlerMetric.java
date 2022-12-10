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

package org.apache.uniffle.storage.handler;

public class ClientReadHandlerMetric {
  private long readBlockNum = 0L;
  private long readLength = 0L;
  private long readUncompressLength = 0L;

  private long skippedReadBlockNum = 0L;
  private long skippedReadLength = 0L;
  private long skippedReadUncompressLength = 0L;

  public long getReadBlockNum() {
    return readBlockNum;
  }

  public void incReadBlockNum() {
    this.readBlockNum++;
  }

  public long getReadLength() {
    return readLength;
  }

  public void incReadLength(long readLength) {
    this.readLength += readLength;
  }

  public long getReadUncompressLength() {
    return readUncompressLength;
  }

  public void incReadUncompressLength(long readUncompressLength) {
    this.readUncompressLength += readUncompressLength;
  }

  public long getSkippedReadBlockNum() {
    return skippedReadBlockNum;
  }

  public void incSkippedReadBlockNum() {
    this.skippedReadBlockNum++;
  }

  public long getSkippedReadLength() {
    return skippedReadLength;
  }

  public void incSkippedReadLength(long skippedReadLength) {
    this.skippedReadLength += skippedReadLength;
  }

  public long getSkippedReadUncompressLength() {
    return skippedReadUncompressLength;
  }

  public void incSkippedReadUncompressLength(long skippedReadUncompressLength) {
    this.skippedReadUncompressLength += skippedReadUncompressLength;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof ClientReadHandlerMetric)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    ClientReadHandlerMetric other = (ClientReadHandlerMetric)obj;
    return readBlockNum == other.getReadBlockNum()
        && readLength == other.getReadLength()
        && readUncompressLength == other.getReadUncompressLength()
        && skippedReadBlockNum == other.getSkippedReadBlockNum()
        && skippedReadLength == other.getSkippedReadLength()
        && skippedReadUncompressLength == other.getSkippedReadUncompressLength();
  }
}
