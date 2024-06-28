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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class CompletableFutureExtensionTest {

  @Test
  public void timeoutExceptionTest() throws ExecutionException, InterruptedException {
    // case1
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return 10;
            });

    CompletableFuture<Integer> wrap =
        CompletableFutureExtension.orTimeout(future, 1, TimeUnit.SECONDS);
    try {
      wrap.get();
      fail();
    } catch (Exception e) {
      if (!(e instanceof ExecutionException) || !(e.getCause() instanceof TimeoutException)) {
        fail();
      }
    }

    // case2
    future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return 10;
            });
    wrap = CompletableFutureExtension.orTimeout(future, 3, TimeUnit.SECONDS);
    assertEquals(10, wrap.get());
  }
}
