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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThreadUtilsTest {

  @Test
  public void shutdownThreadPoolTest() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    AtomicBoolean finished = new AtomicBoolean(false);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                Thread.sleep(100000);
              } catch (InterruptedException interruptedException) {
                interrupted.set(true);
              } finally {
                finished.set(true);
              }
            });
    ThreadUtils.shutdownThreadPool(executorService, 1);
    assertTrue(future.isDone());
    assertFalse(future.isCancelled());
    assertTrue(finished.get());
    assertTrue(interrupted.get());
    assertTrue(executorService.isShutdown());
  }

  @Test
  public void invokeAllTimeoutThreadPoolTest() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Callable<Boolean> slowTask =
        () -> {
          try {
            Thread.sleep(100000);
          } catch (InterruptedException interruptedException) {
            // ignored
          }
          return true;
        };
    Callable<Boolean> fastTask = () -> true;
    List<Future<Boolean>> future =
        executorService.invokeAll(Lists.newArrayList(slowTask, fastTask), 1, TimeUnit.SECONDS);
    assertTrue(future.get(0).isDone());
    assertTrue(future.get(0).isCancelled());
    assertTrue(future.get(1).isDone());
    assertFalse(future.get(1).isCancelled());
    assertFalse(executorService.isShutdown());
  }

  @Test
  public void testExecuteTasksWithFutureHandler() {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Integer> items = Arrays.asList(1, 2, 3, 4, 5);
    Function<Integer, Integer> task = item -> item * 2;
    long timeoutMs = 1000;
    String taskMsg = "Test Task";
    Function<Future<Integer>, Integer> futureHandler =
        future -> {
          try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            return null;
          }
        };

    List<Integer> results =
        ThreadUtils.executeTasks(executorService, items, task, timeoutMs, taskMsg, futureHandler);
    assertEquals(Arrays.asList(2, 4, 6, 8, 10), results);
  }

  @Test
  public void testExecuteTasksWithFutureHandlerAndTimeout() {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Integer> items = Arrays.asList(1, 2, 3, 4, 5);
    AtomicBoolean completed = new AtomicBoolean(false);
    Function<Integer, Integer> task =
        item -> {
          if (item == 3) {
            try {
              Thread.sleep(100000);
              completed.set(true);
            } catch (InterruptedException interruptedException) {
              // ignored
            }
          }
          return item * 2;
        };
    long timeoutMs = 1000;
    String taskMsg = "Test Task";
    String timeoutMsg = "timeout message";
    Function<Future<Integer>, Integer> futureHandler =
        future -> {
          try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            return null;
          }
        };

    List<Integer> results =
        ThreadUtils.executeTasks(
            executorService, items, task, timeoutMs, taskMsg, timeoutMsg, futureHandler);
    assertFalse(completed.get());
    assertEquals(Arrays.asList(2, 4, null, 8, 10), results);
  }
}
