/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
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

package com.tencent.rss.coordinator;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import com.tencent.rss.common.util.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ApplicationManagerTest {

  static {
    CoordinatorMetrics.register();
  }

  private ApplicationManager applicationManager;
  private long appExpiredTime = 2000L;
  private String remotePath1 = "hdfs://path1";
  private String remotePath2 = "hdfs://path2";
  private String remotePath3 = "hdfs://path3";

  @Before
  public void setUp() {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, appExpiredTime);
    applicationManager = new ApplicationManager(conf);
  }

  @Test
  public void refreshTest() {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    Set<String> expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath2);
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());

    remoteStoragePath = remotePath3;
    expectedAvailablePath = Sets.newHashSet(remotePath3);
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());

    remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath3;
    expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath3);
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());

    remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath2);
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
    assertEquals(expectedAvailablePath, applicationManager.getRemoteStoragePathCounter().keySet());
  }

  @Test
  public void storageCounterTest() throws Exception {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2;
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath1).get());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());

    // do inc for remotePath1 to make sure pick storage will be remotePath2 in next call
    applicationManager.incRemoteStorageCounter(remotePath1);
    applicationManager.incRemoteStorageCounter(remotePath1);
    String testApp1 = "testApp1";
    applicationManager.refreshAppId(testApp1);
    assertEquals(remotePath2, applicationManager.pickRemoteStoragePath(testApp1));
    assertEquals(remotePath2, applicationManager.getAppIdToRemoteStoragePath().get(testApp1));
    assertEquals(1, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());
    // return the same value if did the assignment already
    assertEquals(remotePath2, applicationManager.pickRemoteStoragePath(testApp1));
    assertEquals(1, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());

    Thread.sleep(appExpiredTime + 2000);
    assertNull(applicationManager.getAppIdToRemoteStoragePath().get(testApp1));
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().get(remotePath2).get());

    // refresh app1, got remotePath2, then remove remotePath2,
    // it should be existed in counter until it expired
    applicationManager.refreshAppId(testApp1);
    assertEquals(remotePath2, applicationManager.pickRemoteStoragePath(testApp1));
    remoteStoragePath = remotePath1;
    applicationManager.refreshRemoteStorage(remoteStoragePath);
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
    applicationManager.refreshRemoteStorage("");
    assertEquals(0, applicationManager.getAvailableRemoteStoragePath().size());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().size());
  }

  @Test
  public void storageCounterMulThreadTest() throws Exception {
    String remoteStoragePath = remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2
        + Constants.COMMA_SPLIT_CHAR + remotePath3;
    applicationManager.refreshRemoteStorage(remoteStoragePath);
    String appPrefix = "testAppId";

    Thread pickThread1 = new Thread(() -> {
      for (int i = 0; i < 1000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStoragePath(appId);
      }
    });

    Thread pickThread2 = new Thread(() -> {
      for (int i = 1000; i < 2000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStoragePath(appId);
      }
    });

    Thread pickThread3 = new Thread(() -> {
      for (int i = 2000; i < 3000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        applicationManager.pickRemoteStoragePath(appId);
      }
    });
    pickThread1.start();
    pickThread2.start();
    pickThread3.start();
    pickThread1.join();
    pickThread2.join();
    pickThread3.join();
    Thread.sleep(appExpiredTime + 2000);

    applicationManager.refreshRemoteStorage("");
    assertEquals(0, applicationManager.getAvailableRemoteStoragePath().size());
    assertEquals(0, applicationManager.getRemoteStoragePathCounter().size());
  }
}
