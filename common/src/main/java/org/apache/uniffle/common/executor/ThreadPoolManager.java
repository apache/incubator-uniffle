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

package org.apache.uniffle.common.executor;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.metrics.CommonMetrics;

/** The threadPool manager which represents a manager to handle all thread pool executors. */
public class ThreadPoolManager {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolManager.class);

  private static final Map<Object, MeasurableThreadPoolExecutor> THREAD_POOL_MAP =
      new ConcurrentHashMap<>();

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size supplier
   * @param maximumPoolSize the maximum pool size supplier
   * @param keepAliveTime the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    registerThreadPool(name, corePoolSize, maximumPoolSize, keepAliveTime, threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size supplier
   * @param maximumPoolSize the maximum pool size supplier
   * @param keepAliveTime the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @param handler the handler to use when execution is blocked because the thread bounds and queue
   *     capacities are reached
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    registerThreadPool(name, corePoolSize, maximumPoolSize, keepAliveTime, threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Register a thread pool to THREAD_POOL_MAP.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size supplier
   * @param maximumPoolSize the maximum pool size supplier
   * @param keepAliveTime the keep alive time supplier
   * @param threadPoolExecutor the thread pool which will be registered
   */
  public static void registerThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      ThreadPoolExecutor threadPoolExecutor) {
    THREAD_POOL_MAP.put(
        threadPoolExecutor, new MeasurableThreadPoolExecutor(name, threadPoolExecutor));
    LOG.info(
        "{} thread pool, core size:{}, max size:{}, keep alive time:{}",
        name,
        corePoolSize,
        maximumPoolSize,
        keepAliveTime);
  }

  /**
   * Unregister the thread pool executor related to the given key.
   *
   * @param key the key of thread pool executor to unregister
   */
  public static void unregister(Object key) {
    MeasurableThreadPoolExecutor measurableThreadPoolExecutor = THREAD_POOL_MAP.remove(key);
    if (measurableThreadPoolExecutor != null) {
      measurableThreadPoolExecutor.close();
    }
  }

  public static boolean exists(Object key) {
    return THREAD_POOL_MAP.containsKey(key);
  }

  private static class MeasurableThreadPoolExecutor implements Closeable {

    private final String name;

    MeasurableThreadPoolExecutor(String name, ThreadPoolExecutor threadPoolExecutor) {
      this.name = name;
      MeasurableRejectedExecutionHandler measurableRejectedExecutionHandler =
          new MeasurableRejectedExecutionHandler(threadPoolExecutor.getRejectedExecutionHandler());
      threadPoolExecutor.setRejectedExecutionHandler(measurableRejectedExecutionHandler);
      CommonMetrics.addLabeledGauge(
          name + "_ThreadActiveCount", () -> (double) threadPoolExecutor.getActiveCount());
      CommonMetrics.addLabeledGauge(
          name + "_ThreadCurrentCount", () -> (double) threadPoolExecutor.getPoolSize());
      CommonMetrics.addLabeledGauge(
          name + "_ThreadMaxCount", () -> (double) threadPoolExecutor.getMaximumPoolSize());
      CommonMetrics.addLabeledGauge(
          name + "_ThreadMinCount", () -> (double) threadPoolExecutor.getCorePoolSize());
      CommonMetrics.addLabeledGauge(
          name + "_CompleteTaskCount", () -> (double) threadPoolExecutor.getCompletedTaskCount());
      CommonMetrics.addLabeledGauge(
          name + "_ThreadQueueWaitingTaskCount",
          () -> (double) threadPoolExecutor.getQueue().size());
      CommonMetrics.addLabeledGauge(
          name + "_RejectCount", () -> (double) measurableRejectedExecutionHandler.getCount());
    }

    @Override
    public void close() {
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadActiveCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadCurrentCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadMaxCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadMinCount");
      CommonMetrics.unregisterSupplierGauge(name + "_CompleteTaskCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadQueueWaitingTaskCount");
      CommonMetrics.unregisterSupplierGauge(name + "_RejectCount");
    }
  }
}
