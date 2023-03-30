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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class ClientReadHandlerMetric {
  private AtomicLong readBlockNum = new AtomicLong();
  private AtomicLong readLength = new AtomicLong();
  private AtomicLong readUncompressLength = new AtomicLong();

  private AtomicLong skippedReadBlockNum = new AtomicLong();
  private AtomicLong skippedReadLength = new AtomicLong();
  private AtomicLong skippedReadUncompressLength = new AtomicLong();

  public long getReadBlockNum() {
    return readBlockNum.get();
  }

  public void incReadBlockNum() {
    this.readBlockNum.incrementAndGet();
  }

  public long getReadLength() {
    return readLength.get();
  }

  public void incReadLength(long readLength) {
    this.readLength.addAndGet(readLength);
  }

  public long getReadUncompressLength() {
    return readUncompressLength.get();
  }

  public void incReadUncompressLength(long readUncompressLength) {
    this.readUncompressLength.addAndGet(readUncompressLength);
  }

  public long getSkippedReadBlockNum() {
    return skippedReadBlockNum.get();
  }

  public void incSkippedReadBlockNum() {
    this.skippedReadBlockNum.incrementAndGet();
  }

  public long getSkippedReadLength() {
    return skippedReadLength.get();
  }

  public void incSkippedReadLength(long skippedReadLength) {
    this.skippedReadLength.addAndGet(skippedReadLength);
  }

  public long getSkippedReadUncompressLength() {
    return skippedReadUncompressLength.get();
  }

  public void incSkippedReadUncompressLength(long skippedReadUncompressLength) {
    this.skippedReadUncompressLength.addAndGet(skippedReadUncompressLength);
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientReadHandlerMetric that = (ClientReadHandlerMetric) o;
    return readBlockNum.get() == that.getReadBlockNum()
        && readLength.get() == that.getReadLength()
        && readUncompressLength.get() == that.getReadUncompressLength()
        && skippedReadBlockNum.get() == that.getSkippedReadBlockNum()
        && skippedReadLength.get() == that.getSkippedReadLength()
        && skippedReadUncompressLength.get() == that.getSkippedReadUncompressLength();
  }

  @Override
  public int hashCode() {
    return Objects.hash(readBlockNum.get(), readLength.get(), readUncompressLength.get(),
        skippedReadBlockNum.get(), skippedReadLength.get(), skippedReadUncompressLength.get());
  }
}
