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
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleDataResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ComposedClientReadHandlerTest extends ClientHandlerTestBase {

  @Test
  public void testSyncRead() {
    FakeHandler hotHandler = createFakeHandler();
    FakeHandler warmHandler = createFakeHandler();
    FakeHandler coldHandler = createFakeHandler();
    ComposedClientReadHandler composedClientReadHandler =
        new ComposedClientReadHandler(null, hotHandler, warmHandler, coldHandler);

    for (int i = 0; i < 300; i++) {
      ShuffleDataResult dataResult = composedClientReadHandler.readShuffleData();
      assertNotNull(dataResult);
    }
    assertNull(composedClientReadHandler.readShuffleData());
    assertEquals(100, hotHandler.blockIds.size());
    assertEquals(100, warmHandler.blockIds.size());
    assertEquals(100, coldHandler.blockIds.size());
  }

  @Test
  public void testAsyncRead() throws Exception {
    FakeHandler hotHandler = createFakeHandler();
    FakeHandler warmHandler = createFakeHandler();
    FakeHandler coldHandler = createFakeHandler();

    ComposedClientReadHandler composedClientReadHandler =
        new ComposedClientReadHandler(null, hotHandler, warmHandler, coldHandler);

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(10);
    IntStream.range(0, 10).forEach(x -> executorService.submit(() -> {
      IntStream.range(0, 300).forEach(k -> composedClientReadHandler.readShuffleData());
      latch.countDown();
    }));
    latch.await();
    assertNull(composedClientReadHandler.readShuffleData());
    assertEquals(100, hotHandler.blockIds.size());
    assertEquals(100, coldHandler.blockIds.size());
    assertEquals(100, warmHandler.blockIds.size());
  }
}
