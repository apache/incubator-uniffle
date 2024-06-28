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

package org.apache.uniffle.common.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public class CompletableFutureExtension {
  public static <T> CompletableFuture<T> orTimeout(
      CompletableFuture<T> future, long timeout, TimeUnit unit) {
    if (future.isDone()) {
      return future;
    }

    return future.whenComplete(new Canceller(Delayer.delay(new Timeout(future), timeout, unit)));
  }

  static final class Timeout implements Runnable {
    final CompletableFuture<?> future;

    Timeout(CompletableFuture<?> future) {
      this.future = future;
    }

    public void run() {
      if (null != future && !future.isDone()) {
        future.completeExceptionally(new TimeoutException());
      }
    }
  }

  static final class Canceller implements BiConsumer<Object, Throwable> {
    final Future<?> future;

    Canceller(Future<?> future) {
      this.future = future;
    }

    public void accept(Object ignore, Throwable ex) {
      if (null == ex && null != future && !future.isDone()) {
        future.cancel(false);
      }
    }
  }

  static final class Delayer {
    static ScheduledFuture<?> delay(Runnable command, long delay, TimeUnit unit) {
      return delayer.schedule(command, delay, unit);
    }

    static final class DaemonThreadFactory implements ThreadFactory {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("CompletableFutureExtensionDelayScheduler");
        return t;
      }
    }

    static final ScheduledThreadPoolExecutor delayer;

    static {
      delayer = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory());
      delayer.setRemoveOnCancelPolicy(true);
    }
  }
}
