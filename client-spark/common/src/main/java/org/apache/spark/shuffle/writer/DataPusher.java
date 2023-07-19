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

package org.apache.spark.shuffle.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

/**
 * A {@link DataPusher} that is responsible for sending data to remote shuffle servers
 * asynchronously.
 */
public class DataPusher implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPusher.class);

  private final ExecutorService executorService;

  private final ShuffleWriteClient shuffleWriteClient;
  // Must be thread safe
  private final Map<String, Set<Long>> taskToSuccessBlockIds;
  // Must be thread safe
  private final Map<String, Set<Long>> taskToFailedBlockIds;
  private String rssAppId;
  // Must be thread safe
  private final Set<String> failedTaskIds;

  public DataPusher(
      ShuffleWriteClient shuffleWriteClient,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, Set<Long>> taskToFailedBlockIds,
      Set<String> failedTaskIds,
      int threadPoolSize,
      int threadKeepAliveTime) {
    this.shuffleWriteClient = shuffleWriteClient;
    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.taskToFailedBlockIds = taskToFailedBlockIds;
    this.failedTaskIds = failedTaskIds;
    this.executorService =
        new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize * 2,
            threadKeepAliveTime,
            TimeUnit.SECONDS,
            Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
            ThreadUtils.getThreadFactory(this.getClass().getName()));
  }

  public CompletableFuture<Long> send(AddBlockEvent event) {
    if (rssAppId == null) {
      throw new RssException("RssAppId should be set.");
    }
    return CompletableFuture.supplyAsync(
        () -> {
          String taskId = event.getTaskId();
          List<ShuffleBlockInfo> shuffleBlockInfoList = event.getShuffleDataInfoList();
          try {
            SendShuffleDataResult result =
                shuffleWriteClient.sendShuffleData(
                    rssAppId, shuffleBlockInfoList, () -> !isValidTask(taskId));
            putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
            putBlockId(taskToFailedBlockIds, taskId, result.getFailedBlockIds());
          } finally {
            List<Runnable> callbackChain =
                Optional.of(event.getProcessedCallbackChain()).orElse(Collections.EMPTY_LIST);
            for (Runnable runnable : callbackChain) {
              runnable.run();
            }
          }
          return shuffleBlockInfoList.stream()
              .map(x -> x.getFreeMemory())
              .reduce((a, b) -> a + b)
              .get();
        },
        executorService);
  }

  private synchronized void putBlockId(
      Map<String, Set<Long>> taskToBlockIds, String taskAttemptId, Set<Long> blockIds) {
    if (blockIds == null || blockIds.isEmpty()) {
      return;
    }
    taskToBlockIds
        .computeIfAbsent(taskAttemptId, x -> Sets.newConcurrentHashSet())
        .addAll(blockIds);
  }

  public boolean isValidTask(String taskId) {
    return !failedTaskIds.contains(taskId);
  }

  public void setRssAppId(String rssAppId) {
    this.rssAppId = rssAppId;
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      try {
        ThreadUtils.shutdownThreadPool(executorService, 5);
      } catch (InterruptedException interruptedException) {
        LOGGER.error("Errors on shutdown thread pool of [{}].", this.getClass().getSimpleName());
      }
    }
  }
}
