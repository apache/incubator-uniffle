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

import java.io.File;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.util.Constants;

import static org.apache.uniffle.coordinator.ApplicationManager.StrategyName.HEALTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HealthSelectStorageStrategyTest {

  private HealthSelectStorageStrategy healthSelectStorageStrategy;
  private ApplicationManager applicationManager;
  private static final Configuration hdfsConf = new Configuration();
  private static MiniDFSCluster cluster;
  private final long appExpiredTime = 2000L;
  private final String remoteStorage1 = "hdfs://p1";
  private final String remoteStorage2 = "hdfs://p2";
  private final String remoteStorage3 = "hdfs://p3";
  private final Path testFile = new Path("test");

  @TempDir
  private static File remotePath = new File("hdfs://rss");

  @BeforeAll
  public static void setup() {
    CoordinatorMetrics.register();
  }

  @AfterAll
  public static void clear() {
    CoordinatorMetrics.clear();
  }

  @BeforeEach
  public void init() throws Exception {
    setUpHdfs(remotePath.getAbsolutePath());
  }

  public void setUpHdfs(String hdfsPath) throws Exception {
    hdfsConf.set("fs.defaultFS", remotePath.getAbsolutePath());
    hdfsConf.set("dfs.nameservices", "rss");
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsPath);
    cluster = (new MiniDFSCluster.Builder(hdfsConf)).build();
    Thread.sleep(500L);
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, appExpiredTime);
    conf.setLong(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_TIME, 5000);
    conf.set(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SELECT_STRATEGY, HEALTH);
    applicationManager = new ApplicationManager(conf);
    healthSelectStorageStrategy = (HealthSelectStorageStrategy) applicationManager.getSelectStorageStrategy();
    healthSelectStorageStrategy.setConf(hdfsConf);
    Thread.sleep(1000);
  }

  @Test
  public void selectStorageTest() throws Exception {
    FileSystem fs = testFile.getFileSystem(hdfsConf);
    healthSelectStorageStrategy.setFs(fs);

    String remoteStoragePath = remoteStorage1 + Constants.COMMA_SPLIT_CHAR + remoteStorage2;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    //default value is 0
    assertEquals(0,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage1).getRatioValue().get());
    assertEquals(0,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getRatioValue().get());
    String storageHost1 = "p1";
    assertEquals(0.0, CoordinatorMetrics.gaugeInUsedRemoteStorage.get(storageHost1).get(), 0.5);
    String storageHost2 = "p2";
    assertEquals(0.0, CoordinatorMetrics.gaugeInUsedRemoteStorage.get(storageHost2).get(), 0.5);

    // compare with two remote path
    healthSelectStorageStrategy.incRemoteStorageCounter(remoteStorage1);
    healthSelectStorageStrategy.incRemoteStorageCounter(remoteStorage1);
    String testApp1 = "testApp1";
    final long current = System.currentTimeMillis();
    applicationManager.refreshAppId(testApp1);
    fs.create(testFile);
    healthSelectStorageStrategy.sortPathByIORank(remoteStorage2, testFile, current);
    fs.create(testFile);
    healthSelectStorageStrategy.sortPathByIORank(remoteStorage1, testFile, current);
    assertEquals(remoteStorage2, healthSelectStorageStrategy.pickRemoteStorage(testApp1).getPath());
    assertEquals(remoteStorage2, healthSelectStorageStrategy.getAppIdToRemoteStorageInfo().get(testApp1).getPath());
    assertEquals(1,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getAppNum().get());
    // return the same value if did the assignment already
    assertEquals(remoteStorage2, healthSelectStorageStrategy.pickRemoteStorage(testApp1).getPath());
    assertEquals(1,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getAppNum().get());

    // when the expiration time is reached, the app was removed
    Thread.sleep(appExpiredTime + 2000);
    assertNull(healthSelectStorageStrategy.getAppIdToRemoteStorageInfo().get(testApp1));
    assertEquals(0,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getAppNum().get());

    // refresh app1, got remotePath2, then remove remotePath2,
    // it should be existed in counter until it expired
    applicationManager.refreshAppId(testApp1);
    assertEquals(remoteStorage2, healthSelectStorageStrategy.pickRemoteStorage(testApp1).getPath());
    remoteStoragePath = remoteStorage1;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    assertEquals(Sets.newConcurrentHashSet(Sets.newHashSet(remoteStorage1, remoteStorage2)),
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().keySet());
    assertTrue(
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getRatioValue().get() > 0);
    assertEquals(1,
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().get(remoteStorage2).getAppNum().get());
    // app1 is expired, p2 is removed because of counter = 0
    Thread.sleep(appExpiredTime + 2000);
    assertEquals(Sets.newConcurrentHashSet(Sets.newHashSet(remoteStorage1)),
        healthSelectStorageStrategy.getRemoteStoragePathRankValue().keySet());
    // restore previous manually inc for next test case
    healthSelectStorageStrategy.decRemoteStorageCounter(remoteStorage1);
    healthSelectStorageStrategy.decRemoteStorageCounter(remoteStorage1);
    // remove all remote storage
    applicationManager.refreshRemoteStorage("", "");
    assertEquals(0, healthSelectStorageStrategy.getAvailableRemoteStorageInfo().size());
    assertEquals(0, healthSelectStorageStrategy.getRemoteStoragePathRankValue().size());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }

  @Test
  public void selectStorageMulThreadTest() throws Exception {
    FileSystem fs = testFile.getFileSystem(hdfsConf);
    healthSelectStorageStrategy.setFs(fs);
    String remoteStoragePath = remoteStorage1 + Constants.COMMA_SPLIT_CHAR + remoteStorage2
        + Constants.COMMA_SPLIT_CHAR + remoteStorage3;
    applicationManager.refreshRemoteStorage(remoteStoragePath, "");
    String appPrefix = "testAppId";

    Thread pickThread1 = new Thread(() -> {
      for (int i = 0; i < 1000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        healthSelectStorageStrategy.pickRemoteStorage(appId);
      }
    });

    Thread pickThread2 = new Thread(() -> {
      for (int i = 1000; i < 2000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        healthSelectStorageStrategy.pickRemoteStorage(appId);
      }
    });

    Thread pickThread3 = new Thread(() -> {
      for (int i = 2000; i < 3000; i++) {
        String appId = appPrefix + i;
        applicationManager.refreshAppId(appId);
        healthSelectStorageStrategy.pickRemoteStorage(appId);
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
    assertEquals(0, healthSelectStorageStrategy.getAvailableRemoteStorageInfo().size());
    assertEquals(0, healthSelectStorageStrategy.getRemoteStoragePathRankValue().size());
    assertFalse(applicationManager.hasErrorInStatusCheck());
  }
}
