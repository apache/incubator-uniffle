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

package org.apache.uniffle.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.function.ConsumerWithException;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.flush.EventDiscardException;
import org.apache.uniffle.server.flush.EventInvalidException;
import org.apache.uniffle.server.flush.EventRetryException;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.StorageType;

public class DefaultFlushEventHandler implements FlushEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFlushEventHandler.class);

  private final ShuffleServerConf shuffleServerConf;
  private final StorageManager storageManager;
  private Executor localFileThreadPoolExecutor;
  private Executor hadoopThreadPoolExecutor;
  private Executor fallbackThreadPoolExecutor;
  private final StorageType storageType;
  protected final BlockingQueue<ShuffleDataFlushEvent> flushQueue = Queues.newLinkedBlockingQueue();
  private ConsumerWithException<ShuffleDataFlushEvent> eventConsumer;
  private final ShuffleServer shuffleServer;

  private volatile boolean stopped = false;

  public DefaultFlushEventHandler(
      ShuffleServerConf conf,
      StorageManager storageManager,
      ShuffleServer shuffleServer,
      ConsumerWithException<ShuffleDataFlushEvent> eventConsumer) {
    this.shuffleServerConf = conf;
    this.storageType =
        StorageType.valueOf(shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE).name());
    this.storageManager = storageManager;
    this.shuffleServer = shuffleServer;
    this.eventConsumer = eventConsumer;
    initFlushEventExecutor();
  }

  @Override
  public void handle(ShuffleDataFlushEvent event) {
    if (!flushQueue.offer(event)) {
      LOG.error("Flush queue is full, discard event: " + event);
      // We need to release the memory when discarding the event
      event.doCleanup();
      ShuffleServerMetrics.counterTotalDroppedEventNum.inc();
    } else {
      ShuffleServerMetrics.gaugeEventQueueSize.inc();
    }
  }

  /**
   * @param event
   * @param storage
   */
  private void handleEventAndUpdateMetrics(ShuffleDataFlushEvent event, Storage storage) {
    long start = System.currentTimeMillis();
    String appId = event.getAppId();
    ReentrantReadWriteLock.ReadLock readLock =
        shuffleServer.getShuffleTaskManager().getAppReadLock(appId);
    try {
      readLock.lock();
      try {
        eventConsumer.accept(event);
      } finally {
        readLock.unlock();
      }

      if (storage != null) {
        ShuffleServerMetrics.incStorageSuccessCounter(storage.getStorageHost());
      }
      event.doCleanup();

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Flush event:{} successfully in {} ms and release {} bytes",
            event,
            System.currentTimeMillis() - start,
            event.getSize());
      }
    } catch (Exception e) {
      if (e instanceof EventRetryException) {
        event.increaseRetryTimes();
        event.markPended();
        if (storage != null) {
          ShuffleServerMetrics.incStorageRetryCounter(storage.getStorageHost());
        }
        this.handle(event);
        return;
      }

      if (e instanceof EventDiscardException) {
        ShuffleServerMetrics.counterTotalDroppedEventNum.inc();
        ShuffleServerMetrics.counterTotalFailedWrittenEventNum.inc();
        if (storage != null) {
          ShuffleServerMetrics.incStorageFailedCounter(storage.getStorageHost());
        }
        event.doCleanup();
        LOG.error(
            "Flush event: {} failed in {} ms and release {} bytes. This will make data lost.",
            event,
            System.currentTimeMillis() - start,
            event.getSize());
        return;
      }

      if (e instanceof EventInvalidException) {
        event.doCleanup();
        return;
      }

      LOG.error(
          "Unexpected exceptions happened when handling the flush event: {}, due to ", event, e);
      // We need to release the memory when unexpected exceptions happened
      event.doCleanup();
    } finally {
      if (storage != null) {
        if (storage instanceof HadoopStorage) {
          ShuffleServerMetrics.counterHadoopEventFlush.inc();
          ShuffleServerMetrics.gaugeHadoopFlushThreadPoolQueueSize.dec();
        } else if (storage instanceof LocalStorage) {
          ShuffleServerMetrics.counterLocalFileEventFlush.inc();
          ShuffleServerMetrics.gaugeLocalfileFlushThreadPoolQueueSize.dec();
        } else {
          ShuffleServerMetrics.gaugeFallbackFlushThreadPoolQueueSize.dec();
        }
      } else {
        ShuffleServerMetrics.gaugeFallbackFlushThreadPoolQueueSize.dec();
      }

      ShuffleServerMetrics.gaugeEventQueueSize.dec();
    }
  }

  protected void initFlushEventExecutor() {
    if (StorageType.withLocalfile(storageType)) {
      int poolSize =
          shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_LOCALFILE_THREAD_POOL_SIZE);
      localFileThreadPoolExecutor =
          createFlushEventExecutor(poolSize, "LocalFileFlushEventThreadPool");
    }
    if (StorageType.withHadoop(storageType)) {
      int poolSize =
          shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_HADOOP_THREAD_POOL_SIZE);
      hadoopThreadPoolExecutor = createFlushEventExecutor(poolSize, "HadoopFlushEventThreadPool");
    }
    fallbackThreadPoolExecutor = createFlushEventExecutor(5, "FallBackFlushEventThreadPool");
    startEventProcessor();
  }

  private void startEventProcessor() {
    // the thread for flush data
    Thread processEventThread = new Thread(this::eventLoop);
    processEventThread.setName("ProcessEventThread");
    processEventThread.setDaemon(true);
    processEventThread.start();
  }

  protected void eventLoop() {
    while (!stopped && !Thread.currentThread().isInterrupted()) {
      dispatchEvent();
    }
  }

  protected void dispatchEvent() {
    try {
      ShuffleDataFlushEvent event = flushQueue.take();
      Storage storage = storageManager.selectStorage(event);

      Executor dedicatedExecutor = fallbackThreadPoolExecutor;
      // pending event will be delegated to fallback threadPool
      if (!event.isPended()) {
        if (storage instanceof HadoopStorage) {
          dedicatedExecutor = hadoopThreadPoolExecutor;
          ShuffleServerMetrics.gaugeHadoopFlushThreadPoolQueueSize.inc();
        } else if (storage instanceof LocalStorage) {
          dedicatedExecutor = localFileThreadPoolExecutor;
          ShuffleServerMetrics.gaugeLocalfileFlushThreadPoolQueueSize.inc();
        }
      } else {
        dedicatedExecutor = fallbackThreadPoolExecutor;
        ShuffleServerMetrics.gaugeFallbackFlushThreadPoolQueueSize.inc();
      }

      dedicatedExecutor.execute(() -> handleEventAndUpdateMetrics(event, storage));
    } catch (Exception e) {
      LOG.error("Exception happened when pushing events to dedicated event handler.", e);
    }
  }

  protected Executor createFlushEventExecutor(int poolSize, String threadFactoryName) {
    int waitQueueSize =
        shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    LOG.info(
        "CreateFlushPool, poolSize:{}, keepAliveTime:{}, queueSize:{}",
        poolSize,
        keepAliveTime,
        waitQueueSize);
    return new ThreadPoolExecutor(
        poolSize,
        poolSize,
        keepAliveTime,
        TimeUnit.SECONDS,
        waitQueue,
        ThreadUtils.getThreadFactory(threadFactoryName));
  }

  @Override
  public int getEventNumInFlush() {
    return (int) ShuffleServerMetrics.gaugeEventQueueSize.get();
  }

  @Override
  public void stop() {
    stopped = true;
  }

  @VisibleForTesting
  public Executor getFallbackThreadPoolExecutor() {
    return fallbackThreadPoolExecutor;
  }
}
