/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.test;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.File;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.tencent.rss.client.request.RssFetchClientConfRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssFetchClientConfResponse;
import com.tencent.rss.coordinator.CoordinatorConf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FetchClientConfTest extends CoordinatorTestBase {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    File clientConfFile = tmpFolder.newFile();
    FileWriter fileWriter = new FileWriter(clientConfFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.1  1234");
    printWriter.println(" spark.mock.2 true ");
    printWriter.flush();
    printWriter.close();

    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setBoolean("rss.coordinator.dynamicClientConf.enabled", true);
    coordinatorConf.setString("rss.coordinator.dynamicClientConf.path", clientConfFile.getAbsolutePath());
    coordinatorConf.setInteger("rss.coordinator.dynamicClientConf.updateIntervalSec", 10);
    createCoordinatorServer(coordinatorConf);
    startServers();

    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    RssFetchClientConfRequest request = new RssFetchClientConfRequest(2000);
    RssFetchClientConfResponse response = coordinatorClient.fetchClientConf(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    assertEquals(2, response.getClientConf().size());
    assertEquals("1234", response.getClientConf().get("spark.mock.1"));
    assertEquals("true", response.getClientConf().get("spark.mock.2"));
    assertNull(response.getClientConf().get("spark.mock.3"));
    shutdownServers();

    // dynamic client conf is disabled by default
    coordinatorConf = getCoordinatorConf();
    coordinatorConf.setString("rss.coordinator.dynamicClientConf.path", clientConfFile.getAbsolutePath());
    coordinatorConf.setInteger("rss.coordinator.dynamicClientConf.updateIntervalSec", 10);
    createCoordinatorServer(coordinatorConf);
    startServers();
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    request = new RssFetchClientConfRequest(2000);
    response = coordinatorClient.fetchClientConf(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    assertEquals(0, response.getClientConf().size());
    shutdownServers();

    // Fetch conf will not throw exception even if the request fails
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    request = new RssFetchClientConfRequest(10);
    response = coordinatorClient.fetchClientConf(request);
    assertEquals(ResponseStatusCode.INTERNAL_ERROR, response.getStatusCode());
    assertEquals(0, response.getClientConf().size());
  }
}
