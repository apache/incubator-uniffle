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
import java.util.function.Consumer;

import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;
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
  private final StorageType storageType;
  protected final BlockingQueue<ShuffleDataFlushEvent> flushQueue = Queues.newLinkedBlockingQueue();
  private Consumer<ShuffleDataFlushEvent> eventConsumer;

  private volatile boolean stopped = false;

  public DefaultFlushEventHandler(
      ShuffleServerConf conf,
      StorageManager storageManager,
      Consumer<ShuffleDataFlushEvent> eventConsumer) {
    this.shuffleServerConf = conf;
    this.storageType = StorageType.valueOf(shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE));
    this.storageManager = storageManager;
    this.eventConsumer = eventConsumer;
    initFlushEventExecutor();
  }

  @Override
  public void handle(ShuffleDataFlushEvent event) {
    if (!flushQueue.offer(event)) {
      LOG.warn("Flush queue is full, discard event: " + event);
    } else {
      ShuffleServerMetrics.gaugeEventQueueSize.inc();
    }
  }

  private void handleEventAndUpdateMetrics(ShuffleDataFlushEvent event, boolean isLocalFile) {
    try {
      eventConsumer.accept(event);
    } finally {
      if (isLocalFile) {
        ShuffleServerMetrics.counterLocalFileEventFlush.inc();
      } else {
        ShuffleServerMetrics.counterHadoopEventFlush.inc();
      }
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
      processNextEvent();
    }
  }

  protected void processNextEvent() {
    try {
      ShuffleDataFlushEvent event = flushQueue.take();
      Storage storage = storageManager.selectStorage(event);
      if (storage instanceof HadoopStorage) {
        hadoopThreadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event, false));
      } else if (storage instanceof LocalStorage) {
        localFileThreadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event, true));
      } else {
        throw new RssException("Unexpected storage type!");
      }
    } catch (Exception e) {
      LOG.error("Exception happened when process event.", e);
    }
  }

  protected Executor createFlushEventExecutor(int poolSize, String threadFactoryName) {
    int waitQueueSize =
        shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
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
    return flushQueue.size();
  }

  @Override
  public void stop() {
    stopped = true;
  }
}
