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

package org.apache.uniffle.common.netty.protocol;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.FileRegion;

public class CompositeFileRegion extends AbstractFileRegion {
  private final FileRegion[] regions;
  private long totalSize = 0;
  private long transferred = 0;

  public CompositeFileRegion(FileRegion... regions) {
    this.regions = regions;
    for (FileRegion region : regions) {
      totalSize += region.count();
    }
  }

  @Override
  public long position() {
    return transferred;
  }

  @Override
  public long count() {
    return totalSize;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    long totalTransferred = 0;

    for (FileRegion region : regions) {
      if (position >= region.count()) {
        position -= region.count();
      } else {
        long transferredNow = region.transferTo(target, position);
        totalTransferred += transferredNow;
        transferred += transferredNow;

        if (transferredNow < region.count() - position) {
          break;
        }
        position = 0;
      }
    }

    return totalTransferred;
  }

  @Override
  public long transferred() {
    return transferred;
  }

  @Override
  public AbstractFileRegion retain() {
    super.retain();
    for (FileRegion region : regions) {
      region.retain();
    }
    return this;
  }

  @Override
  public AbstractFileRegion retain(int increment) {
    super.retain(increment);
    for (FileRegion region : regions) {
      region.retain(increment);
    }
    return this;
  }

  @Override
  public boolean release() {
    super.release();
    boolean released = true;
    for (FileRegion region : regions) {
      if (!region.release()) {
        released = false;
      }
    }
    return released;
  }

  @Override
  public boolean release(int decrement) {
    super.release(decrement);
    boolean released = true;
    for (FileRegion region : regions) {
      if (!region.release(decrement)) {
        released = false;
      }
    }
    return released;
  }

  @Override
  protected void deallocate() {}

  @Override
  public AbstractFileRegion touch() {
    super.touch();
    for (FileRegion region : regions) {
      region.touch();
    }
    return this;
  }

  @Override
  public AbstractFileRegion touch(Object hint) {
    super.touch(hint);
    for (FileRegion region : regions) {
      region.touch(hint);
    }
    return this;
  }
}
