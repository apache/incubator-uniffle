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

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Supplier for T cacheable and autocloseable with delay By using ExpiringCloseableSupplier to
 * obtain an object, manual closure may not be necessary.
 */
public class ExpiringCloseableSupplier<T extends CloseStateful>
    implements Supplier<T>, Serializable {
  private static final long serialVersionUID = 0;
  private static final Logger LOG = LoggerFactory.getLogger(ExpiringCloseableSupplier.class);
  private static final int DEFAULT_DELAY_CLOSE_INTERVAL = 60000;
  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> future;
  private volatile T t;
  private final Supplier<T> delegate;
  private transient volatile long freshTime;
  private final long delayCloseInterval;

  public ExpiringCloseableSupplier(Supplier<T> delegate) {
    this(delegate, DEFAULT_DELAY_CLOSE_INTERVAL);
  }

  public ExpiringCloseableSupplier(Supplier<T> delegate, long delayCloseInterval) {
    this.delegate = delegate;
    this.delayCloseInterval = delayCloseInterval;
  }

  public synchronized T get() {
    freshTime = System.currentTimeMillis();
    if (t == null || t.isClosed()) {
      t = delegate.get();
      startDelayCloseScheduler();
    }
    return t;
  }

  public synchronized void close() {
    try {
      if (t != null && !t.isClosed()) {
        t.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close {} the resource", t.getClass().getName(), e);
    } finally {
      t = null;
      freshTime = 0;
      shutdownDelayCloseScheduler();
    }
  }

  public void tryClose() {
    if (System.currentTimeMillis() - freshTime > delayCloseInterval) {
      this.close();
    }
  }

  private void startDelayCloseScheduler() {
    shutdownDelayCloseScheduler();
    executor = Executors.newSingleThreadScheduledExecutor();
    future =
        executor.scheduleAtFixedRate(
            this::tryClose, delayCloseInterval, delayCloseInterval, TimeUnit.MILLISECONDS);
  }

  private void shutdownDelayCloseScheduler() {
    if (future != null && !future.isDone()) {
      future.cancel(false);
    }
    if (executor != null && !executor.isShutdown()) {
      executor.shutdown();
    }
  }
}
