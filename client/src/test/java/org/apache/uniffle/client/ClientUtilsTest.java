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

package org.apache.uniffle.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.RssUtils;

import static org.apache.uniffle.client.util.ClientUtils.waitUntilDoneOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ClientUtilsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtilsTest.class);

  private ExecutorService executorService = Executors.newFixedThreadPool(10);

  @Test
  public void testGenerateTaskIdBitMap() {
    int partitionId = 1;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    BlockIdSet blockIdMap = BlockIdSet.empty();
    int taskSize = 10;
    long[] except = new long[taskSize];
    for (int i = 0; i < taskSize; i++) {
      except[i] = i;
      for (int j = 0; j < 100; j++) {
        long blockId = layout.getBlockId(j, partitionId, i);
        blockIdMap.add(blockId);
      }
    }
    Roaring64NavigableMap taskIdBitMap =
        RssUtils.generateTaskIdBitMap(blockIdMap, new DefaultIdHelper(layout));
    assertEquals(taskSize, taskIdBitMap.getLongCardinality());
    LongIterator longIterator = taskIdBitMap.getLongIterator();
    for (int i = 0; i < taskSize; i++) {
      assertEquals(except[i], longIterator.next());
    }
  }

  private List<CompletableFuture<Boolean>> getFutures(boolean fail) {
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final int index = i;
      CompletableFuture<Boolean> future =
          CompletableFuture.supplyAsync(
              () -> {
                if (index == 2) {
                  try {
                    Thread.sleep(3000);
                  } catch (InterruptedException interruptedException) {
                    LOGGER.info("Capture the InterruptedException");
                    return false;
                  }
                  LOGGER.info("Finished index: " + index);
                  return true;
                }
                if (fail && index == 1) {
                  return false;
                }
                return true;
              },
              executorService);
      futures.add(future);
    }
    return futures;
  }

  @Test
  public void testWaitUntilDoneOrFail() {
    // case1: enable fail fast
    List<CompletableFuture<Boolean>> futures1 = getFutures(true);
    Awaitility.await()
        .timeout(2, TimeUnit.SECONDS)
        .until(() -> !waitUntilDoneOrFail(futures1, true));

    // case2: disable fail fast
    List<CompletableFuture<Boolean>> futures2 = getFutures(true);
    try {
      Awaitility.await()
          .timeout(2, TimeUnit.SECONDS)
          .until(() -> !waitUntilDoneOrFail(futures2, false));
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case3: all succeed
    List<CompletableFuture<Boolean>> futures3 = getFutures(false);
    Awaitility.await()
        .timeout(4, TimeUnit.SECONDS)
        .until(() -> waitUntilDoneOrFail(futures3, true));
  }

  @Test
  public void testValidateClientType() {
    String clientType = "GRPC_NETTY";
    ClientUtils.validateClientType(clientType);
    clientType = "test";
    try {
      ClientUtils.validateClientType(clientType);
      fail();
    } catch (Exception e) {
      // Ignore
    }
  }
}
