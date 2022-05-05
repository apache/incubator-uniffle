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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.tencent.rss.client.request.RssFetchClientConfRequest;
import com.tencent.rss.client.request.RssFetchRemoteStorageRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssFetchClientConfResponse;
import com.tencent.rss.client.response.RssFetchRemoteStorageResponse;
import com.tencent.rss.coordinator.ApplicationManager;
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
    coordinatorConf.setBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH,
        clientConfFile.getAbsolutePath());
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

  @Test
  public void testFetchRemoteStorage() throws Exception {
    String remotePath1 = "hdfs://path1";
    String remotePath2 = "hdfs://path2";
    File cfgFile = tmpFolder.newFile();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), remotePath1);
    writeRemoteStorageConf(cfgFile, dynamicConf);

    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    coordinatorConf.setInteger(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, 3);
    createCoordinatorServer(coordinatorConf);
    startServers();

    waitForUpdate(Sets.newHashSet(remotePath1), coordinators.get(0).getApplicationManager());
    String appId = "testFetchRemoteStorageApp";
    RssFetchRemoteStorageRequest request = new RssFetchRemoteStorageRequest(appId);
    RssFetchRemoteStorageResponse response = coordinatorClient.fetchRemoteStorage(request);
    assertEquals(remotePath1, response.getRemoteStorage());

    // update remote storage info
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), remotePath2);
    writeRemoteStorageConf(cfgFile, dynamicConf);
    waitForUpdate(Sets.newHashSet(remotePath2), coordinators.get(0).getApplicationManager());
    request = new RssFetchRemoteStorageRequest(appId);
    response = coordinatorClient.fetchRemoteStorage(request);
    // remotePath1 will be return because (appId -> remote storage path) is in cache
    assertEquals(remotePath1, response.getRemoteStorage());

    request = new RssFetchRemoteStorageRequest(appId + "another");
    response = coordinatorClient.fetchRemoteStorage(request);
    // got the remotePath2 for new appId
    assertEquals(remotePath2, response.getRemoteStorage());
  }

  private void waitForUpdate(
      Set<String> expectedAvailablePath,
      ApplicationManager applicationManager) throws Exception {
    int maxAttempt = 10;
    int attempt = 0;
    while (true) {
      if (attempt > maxAttempt) {
        throw new RuntimeException("Timeout when update configuration");
      }
      Thread.sleep(1000);
      try {
        assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
        break;
      } catch (Throwable e) {
        // ignore
      }
      attempt++;
    }
  }
}
