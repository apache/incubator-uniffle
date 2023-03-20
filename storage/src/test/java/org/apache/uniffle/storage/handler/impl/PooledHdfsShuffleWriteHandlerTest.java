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

package org.apache.uniffle.storage.handler.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PooledHdfsShuffleWriteHandlerTest {

  static class FakedShuffleWriteHandler implements ShuffleWriteHandler {
    private List<Integer> invokedList;
    private int index;
    private Runnable execution;

    FakedShuffleWriteHandler(List<Integer> invokedList, int index, Runnable runnable) {
      this.invokedList = invokedList;
      this.index = index;
      this.execution = runnable;
    }

    @Override
    public void write(List<ShufflePartitionedBlock> shuffleBlocks) throws Exception {
      execution.run();
      invokedList.add(index);
    }
  }

  @Test
  public void writeSameFileWhenNoRaceCondition() throws Exception {
    int concurrency = 5;
    CopyOnWriteArrayList<Integer> invokedIndexes = new CopyOnWriteArrayList<>();
    LinkedBlockingDeque deque = new LinkedBlockingDeque(concurrency);
    for (int i = 0; i < concurrency; i ++) {
      deque.addFirst(
          new FakedShuffleWriteHandler(invokedIndexes, i, () -> {
            try {
              Thread.sleep(100);
            } catch (InterruptedException interruptedException) {
              // ignore
            }
          })
      );
    }
    PooledHdfsShuffleWriteHandler handler = new PooledHdfsShuffleWriteHandler(deque);

    for (int i = 0; i < 10; i++) {
      handler.write(Collections.emptyList());
    }
    assertEquals(10, invokedIndexes.size());
    assertEquals(10, invokedIndexes.stream().filter(x -> x == 4).count());
  }

  @Test
  public void concurrentWrite() throws InterruptedException {
    int concurrency = 5;
    CopyOnWriteArrayList<Integer> invokedIndexes = new CopyOnWriteArrayList<>();
    LinkedBlockingDeque deque = new LinkedBlockingDeque(concurrency);
    for (int i = 0; i < concurrency; i ++) {
      deque.addFirst(
          new FakedShuffleWriteHandler(invokedIndexes, i, () -> {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException interruptedException) {
              // ignore
            }
          })
      );
    }
    PooledHdfsShuffleWriteHandler handler = new PooledHdfsShuffleWriteHandler(deque);

    ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
    for (int i = 0; i < concurrency; i++) {
      executorService.submit(() -> {
        try {
          handler.write(Collections.emptyList());
        } catch (Exception e) {
          // ignore
          e.printStackTrace();
        }
      });
    }
    Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> invokedIndexes.size() == concurrency);
    executorService.shutdownNow();
  }
}
