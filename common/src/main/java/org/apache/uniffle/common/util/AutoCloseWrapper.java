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

package org.apache.uniffle.common.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class AutoCloseWrapper<T extends Closeable> implements Closeable {
  private static final AtomicLong REF_COUNTER = new AtomicLong();
  private volatile T t;
  private Supplier<T> cf;

  public AutoCloseWrapper(Supplier<T> cf) {
    this.cf = cf;
  }

  public T get() {
    synchronized (this) {
      if (t == null) {
        synchronized (this) {
          t = cf.get();
        }
      }
    }
    REF_COUNTER.incrementAndGet();
    return t;
  }

  @Override
  public void close() throws IOException {
    while (true) {
      long count = REF_COUNTER.get();
      if (count == 0 || t == null) {
        break;
      }
      if (REF_COUNTER.compareAndSet(count, count - 1)) {
        if (count == 1) {
          try {
            t.close();
          } catch (Exception e) {
            throw new IOException("Failed to close the resource", e);
          } finally {
            t = null;
          }
          break;
        }
      }
    }
  }
}
