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

package org.apache.uniffle.coordinator;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.Constants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApplicationManagerTest {

  private ApplicationManager applicationManager;
  private long appExpiredTime = 2000L;
  private String remotePath1 = "hdfs://path1";
  private String remotePath2 = "hdfs://path2";
  private String remotePath3 = "hdfs://path3";
  private String remoteStorageConf = "path1,k1=v1,k2=v2;path2,k3=v3";

  @BeforeAll
  public static void setup() {
    CoordinatorMetrics.register();
  }

  @AfterAll
  public static void clear() {
    CoordinatorMetrics.clear();
  }

  @BeforeEach
  public void setUp() {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, appExpiredTime);
    applicationManager = new ApplicationManager(conf);
  }

  @Test
  public void refreshTest() {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    Set<String> expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath2);
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStorageInfo().keySet());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());

    remoteStoragePath = remotePath3;
    expectedAvailablePath = Sets.newHashSet(remotePath3);
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStorageInfo().keySet());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());

    remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath3;
    expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath3);
    applicationManager.refreshRemoteStorage(remoteStoragePath, remoteStorageConf);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStorageInfo().keySet());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());
    Map<String, RemoteStorageInfo> storages = applicationManager.getAvailableRemoteStorageInfo();
    RemoteStorageInfo remoteStorageInfo = storages.get(remotePath1);
    assertEquals(2, remoteStorageInfo.getConfItems().size());
    assertEquals("v1", remoteStorageInfo.getConfItems().get("k1"));
    assertEquals("v2", remoteStorageInfo.getConfItems().get("k2"));
    remoteStorageInfo = storages.get(remotePath3);
    assertTrue(remoteStorageInfo.getConfItems().isEmpty());

    remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath2);
    applicationManager.refreshRemoteStorage(remoteStoragePath, remoteStorageConf);
    remoteStorageInfo = storages.get(remotePath2);
    assertEquals(1, remoteStorageInfo.getConfItems().size());
    assertEquals("v3", remoteStorageInfo.getConfItems().get("k3"));
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStorageInfo().keySet());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }

  @Test
  public void storageCounterTest() throws Exception {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath1).get());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());
    String storageHost1 = "path1";
    assertEquals(0.0, CoordinatorMetrics.gaugeInUsedRemoteStorage.get(storageHost1).get(), 0.5);
    String storageHost2 = "path2";
    assertEquals(0.0, CoordinatorMetrics.gaugeInUsedRemoteStorage.get(storageHost2).get(), 0.5);

    // do inc for remotePath1 to make sure pick storage will be remotePath2 in next call
    applicationManager.incRemoteStorageCounter(remotePath1);
    applicationManager.incRemoteStorageCounter(remotePath1);
    String testApp1 = "testApp1";
    applicationManager.refreshAppId(testApp1);
    assertEquals(remotePath2, applicationManager.pickRemoteStorage(testApp1).getPath());
    assertEquals(remotePath2, applicationManager.getAppIdToRemoteStorageInfo().get(testApp1).getPath());
    assertEquals(1, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());
    // return the same value if did the assignment already
    assertEquals(remotePath2, applicationManager.pickRemoteStorage(testApp1).getPath());
    assertEquals(1, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());

    Thread.sleep(appExpiredTime + 2000);
    assertNull(applicationManager.getAppIdToRemoteStorageInfo().get(testApp1));
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());
    assertEquals(0.0, CoordinatorMetrics.gaugeInUsedRemoteStorage.get(storageHost2).get(), 0.5);

    // refresh app1, got remotePath2, then remove remotePath2,
    // it should be existed in counter until it expired
    applicationManager.refreshAppId(testApp1);
    assertEquals(remotePath2, applicationManager.pickRemoteStorage(testApp1).getPath());
    remoteStoragePath = remotePath1;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    assertEquals(Sets.newConcurrentHashSet(Sets.newHashSet(remotePath1, remotePath2)),
        applicationManager.getRemoteStoragePathCounter().keySet());
    assertEquals(1, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());
    // app1 is expired, remotePath2 is removed because of counter = 0
    Thread.sleep(appExpiredTime + 2000);
    assertEquals(Sets.newConcurrentHashSet(Sets.newHashSet(remotePath1)),
        applicationManager.getRemoteStoragePathCounter().keySet());

    // restore previous manually inc for next test case
    applicationManager.decRemoteStorageCounter(remotePath1);
    applicationManager.decRemoteStorageCounter(remotePath1);
    // remove all remote storage
    applicationManager.refreshRemoteStorage("", "");
    assertEquals(0, applicationManager.getAvailableRemoteStorageInfo().size());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().size());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }

  @Test
  public void clearWithoutRemoteStorageTest() throws Exception {
    // test case for storage type without remote storage,
    // NPE shouldn't happen when clear the resource
    String testApp = "clearWithoutRemoteStorageTest";
    applicationManager.refreshAppId(testApp);
    // just set a value != 0, it should be reset to 0 if everything goes well
    CoordinatorMetrics.gaugeRunningAppNum.set(100.0);
    assertEquals(1, applicationManager.getAppIds().size());
    Thread.sleep(appExpiredTime + 2000);
    assertEquals(0, applicationManager.getAppIds().size());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }

  @Test
  public void storageCounterMulThreadTest() throws Exception {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2
        + Constants.COMMA_SPLIT_CHAR + remotePath3;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    String appPrefix = "testAppId";

    Thread pickThread1 = new Thread(() -> {
      for (int i = 0; i < 1000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStorage(appId);
      }
    });

    Thread pickThread2 = new Thread(() -> {
      for (int i = 1000; i < 2000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStorage(appId);
      }
    });

    Thread pickThread3 = new Thread(() -> {
      for (int i = 2000; i < 3000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStorage(appId);
      }
    });
    pickThread1.start();
    pickThread2.start();
    pickThread3.start();
    pickThread1.join();
    pickThread2.join();
    pickThread3.join();
    Thread.sleep(appExpiredTime + 2000);

    applicationManager.refreshRemoteStorage("", "");
    assertEquals(0, applicationManager.getAvailableRemoteStorageInfo().size());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().size());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }
}
