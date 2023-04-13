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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DataSkippableReadHandlerTest extends ClientHandlerTestBase {

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 20, 100})
  public void testAsyncRead(int processedIdSize) throws Exception {
    FakeHandler fakeHandler = createFakeHandler();
    for (int i = 0; i < processedIdSize; i++) {
      fakeHandler.processBlockIds.addLong(i);
    }

    CountDownLatch latch = new CountDownLatch(10);
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      executorService.submit(() -> {
        for (int k = 0; k < 30; k++) {
          fakeHandler.readShuffleData();
        }
        latch.countDown();
      });
    }
    latch.await();
    assertEquals(100 - processedIdSize, fakeHandler.blockIds.size());
    assertNull(fakeHandler.readShuffleData());
    executorService.shutdownNow();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 20, 100})
  public void testSyncRead(int processedIdSize) {
    FakeHandler handler = createFakeHandler();
    for (int i = 0; i < processedIdSize; i++) {
      handler.processBlockIds.addLong(i);
    }
    for (int i = processedIdSize; i < 100; i++) {
      assertNotNull(handler.readShuffleData());
      assertEquals(i - processedIdSize + 1, handler.blockIds.size());
      assertEquals(i, handler.blockIds.get(i - processedIdSize));
    }
    assertNull(handler.readShuffleData());
  }
}
