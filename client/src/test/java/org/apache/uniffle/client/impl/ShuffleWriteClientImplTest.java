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

package org.apache.uniffle.client.impl;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.response.ResponseStatusCode;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.apache.uniffle.client.impl.ShuffleWriteClientImpl.waitUntilDoneOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShuffleWriteClientImplTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleWriteClientImplTest.class);
  private ExecutorService executorService = Executors.newFixedThreadPool(10);

  private List<Future<Boolean>> getFutures(boolean fail) {
    List<Future<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final int index = i;
      Future<Boolean> future = executorService.submit(() -> {
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
      });
      futures.add(future);
    }
    return futures;
  }

  @Test
  public void testWaitUntilDoneOrFail() {
    // case1: enable fail fast
    List<Future<Boolean>> futures1 = getFutures(true);
    Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> !waitUntilDoneOrFail(futures1, true));

    // case2: disable fail fast
    List<Future<Boolean>> futures2 = getFutures(true);
    try {
      Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> !waitUntilDoneOrFail(futures2, false));
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case3: all succeed
    List<Future<Boolean>> futures3 = getFutures(false);
    Awaitility.await().timeout(4, TimeUnit.SECONDS).until(() -> waitUntilDoneOrFail(futures3, true));
  }

  @Test
  public void testAbandonEventWhenTaskFailed() {
    ShuffleWriteClientImpl shuffleWriteClient =
        new ShuffleWriteClientImpl("GRPC", 3, 2000, 4, 1, 1, 1, true, 1, 1, 10, 10);
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());

    when(mockShuffleServerClient.sendShuffleData(any())).thenAnswer((Answer<String>) invocation -> {
      Thread.sleep(50000);
      return "ABCD1234";
    });

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList(new ShuffleBlockInfo(
        0, 0, 10, 10, 10, new byte[]{1}, shuffleServerInfoList, 10, 100, 0));

    // It should directly exit and wont do rpc request.
    Awaitility.await().timeout(1, TimeUnit.SECONDS).until(() -> {
      spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> false);
      return true;
    });
  }

  @Test
  public void testSendData() {
    ShuffleWriteClientImpl shuffleWriteClient =
        new ShuffleWriteClientImpl("GRPC", 3, 2000, 4, 1, 1, 1, true, 1, 1, 10, 10);
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any())).thenReturn(
        new RssSendShuffleDataResponse(ResponseStatusCode.NO_BUFFER));

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList(new ShuffleBlockInfo(
        0, 0, 10, 10, 10, new byte[]{1}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result = spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> true);

    assertTrue(result.getFailedBlockIds().contains(10L));
  }

  @Test
  public void testRegisterAndUnRegisterShuffleServer() {
    ShuffleWriteClientImpl shuffleWriteClient =
        new ShuffleWriteClientImpl("GRPC", 3, 2000, 4, 1, 1, 1, true, 1, 1, 10, 10);
    String appId1 = "testRegisterAndUnRegisterShuffleServer-1";
    String appId2 = "testRegisterAndUnRegisterShuffleServer-2";
    ShuffleServerInfo server1 = new ShuffleServerInfo("host1-0", "host1", 0);
    ShuffleServerInfo server2 = new ShuffleServerInfo("host2-0", "host2", 0);
    ShuffleServerInfo server3 = new ShuffleServerInfo("host3-0", "host3", 0);
    shuffleWriteClient.addShuffleServer(appId1, 0, server1);
    shuffleWriteClient.addShuffleServer(appId1, 1, server2);
    shuffleWriteClient.addShuffleServer(appId2, 1, server3);
    assertEquals(2, shuffleWriteClient.getAllShuffleServers(appId1).size());
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId2).size());
    shuffleWriteClient.addShuffleServer(appId1, 1, server1);
    shuffleWriteClient.unregisterShuffle(appId1, 1);
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId1).size());
  }
}
