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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ExitUtils.ExitException;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerTest {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

  @Test
  public void startTest() {
    try {
      ShuffleServer ss1 = createShuffleServer();
      ss1.start();
      ShuffleServerConf serverConf = ss1.getShuffleServerConf();
      ExitUtils.disableSystemExit();
      ShuffleServer ss2 = new ShuffleServer(serverConf);
      String expectMessage = "Fail to start jetty http server";
      final int expectStatus = 1;
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      serverConf.setInteger("rss.jetty.http.port", 9529);
      ss2 = new ShuffleServer(serverConf);
      expectMessage = "Fail to start grpc server";
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      final Thread t = new Thread(null, () -> {
        throw new AssertionError("TestUncaughtException");
      }, "testThread");
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }

  @Test
  public void decommissionTest() throws Exception {
    ShuffleServer shuffleServer = createShuffleServer();
    assertEquals(ServerStatus.NORMAL_STATUS, shuffleServer.getServerStatus());
    try {
      shuffleServer.cancelDecommission();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e instanceof InvalidRequestException);
    }
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "decommissionTest_appId";
    shuffleTaskManager.registerShuffle(appId, 0, Lists.newArrayList(), new RemoteStorageInfo("/tmp"), "");
    shuffleServer.decommission();
    try {
      assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());
      shuffleServer.decommission();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e instanceof InvalidRequestException);
    }
    shuffleServer.cancelDecommission();
    shuffleTaskManager.removeShuffleDataSync(appId, 0);
    assertEquals(ServerStatus.NORMAL_STATUS, shuffleServer.getServerStatus());
    shuffleServer.decommission();
    Awaitility.await().timeout(10, TimeUnit.SECONDS).until(
                () -> !shuffleServer.isRunning());
  }

  private ShuffleServer createShuffleServer() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger(ShuffleServerConf.RPC_SERVER_PORT, 9527);
    serverConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.setInteger(ShuffleServerConf.JETTY_HTTP_PORT, 9528);
    serverConf.setString(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:0");
    serverConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/tmp/null"));
    serverConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    serverConf.setLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 100);
    serverConf.setLong(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 10);
    return new ShuffleServer(serverConf);
  }
}
