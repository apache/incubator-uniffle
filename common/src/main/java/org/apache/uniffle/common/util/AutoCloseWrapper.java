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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.uniffle.common.exception.RssException;

public class AutoCloseWrapper<T extends Closeable> implements Closeable {
  private static final AtomicInteger REF_COUNTER = new AtomicInteger();
  private volatile T t;
  private Supplier<T> cf;

  public AutoCloseWrapper(Supplier<T> cf) {
    this.cf = cf;
  }

  public T get() {
    if (t == null) {
      synchronized (this) {
        if (t == null) {
          t = cf.get();
        }
      }
    }
    REF_COUNTER.incrementAndGet();
    return t;
  }

  @Override
  public synchronized void close() throws IOException {
    int count = REF_COUNTER.get();
    if (count == 0 || t == null) {
      return;
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
      }
    }
  }

  public void forceClose() throws IOException {
    while (t != null) {
      this.close();
    }
  }

  @VisibleForTesting
  public int getRefCount() {
    return REF_COUNTER.get();
  }

  public static <T, X extends Closeable> T run(
      AutoCloseWrapper<X> autoCloseWrapper, AutoCloseCmd<T, X> cmd) {
    try (AutoCloseWrapper<X> wrapper = autoCloseWrapper) {
      return cmd.execute(wrapper.get());
    } catch (IOException e) {
      throw new RssException("Error closing client with error:", e);
    }
  }

  public interface AutoCloseCmd<T, X> {
    T execute(X x);
  }
}
