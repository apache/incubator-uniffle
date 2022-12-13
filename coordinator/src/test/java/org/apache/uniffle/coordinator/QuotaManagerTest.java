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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QuotaManager is a manager for resource restriction.
 */
public class QuotaManagerTest {
  private static final Configuration hdfsConf = new Configuration();
  private static MiniDFSCluster cluster;
  @TempDir
  private static File remotePath = new File("hdfs://rss");

  @BeforeEach
  public void setUp() throws IOException {
    hdfsConf.set("fs.defaultFS", remotePath.getAbsolutePath());
    hdfsConf.set("dfs.nameservices", "rss");
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remotePath.getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(hdfsConf)).build();
  }

  @AfterAll
  public static void clear() {
    cluster.close();
  }

  @Test
  public void testDetectUserResource() throws Exception {
    final String quotaFile =
        new Path(remotePath.getAbsolutePath()).getFileSystem(hdfsConf).getName() + "/quotaFile.properties";
    final FSDataOutputStream fsDataOutputStream =
        new Path(remotePath.toString()).getFileSystem(hdfsConf).create(new Path(quotaFile));
    String quota1 = "user1 =10";
    String quota2 = "user2= 20";
    String quota3 = "user3 = 30";
    fsDataOutputStream.write(quota1.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write(quota2.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write(quota3.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.flush();
    fsDataOutputStream.close();
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH,
        quotaFile);
    ApplicationManager applicationManager = new ApplicationManager(conf);
    Thread.sleep(500);

    Integer user1 = applicationManager.getDefaultUserApps().get("user1");
    Integer user2 = applicationManager.getDefaultUserApps().get("user2");
    Integer user3 = applicationManager.getDefaultUserApps().get("user3");
    assertEquals(user1, 10);
    assertEquals(user2, 20);
    assertEquals(user3, 30);
  }

  @Test
  public void testQuotaManagerWithoutAccessQuotaChecker() throws Exception {
    final String quotaFile =
        new Path(remotePath.getAbsolutePath()).getFileSystem(hdfsConf).getName() + "/quotaFile.properties";
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH,
        quotaFile);
    conf.set(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        Lists.newArrayList("org.apache.uniffle.coordinator.access.checker.AccessClusterLoadChecker"));
    ApplicationManager applicationManager = new ApplicationManager(conf);
    Thread.sleep(500);
    // it didn't detectUserResource because `org.apache.unifle.coordinator.AccessQuotaChecker` is not configured
    assertNull(applicationManager.getQuotaManager());
  }

  @Test
  public void testCheckQuota() throws Exception {
    final String quotaFile =
        new Path(remotePath.getAbsolutePath()).getFileSystem(hdfsConf).getName() + "/quotaFile.properties";
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH,
        quotaFile);
    final ApplicationManager applicationManager = new ApplicationManager(conf);
    final AtomicInteger uuid = new AtomicInteger();
    Map<String, Long> uuidAndTime = new ConcurrentHashMap<>();
    uuidAndTime.put(String.valueOf(uuid.incrementAndGet()), System.currentTimeMillis());
    uuidAndTime.put(String.valueOf(uuid.incrementAndGet()), System.currentTimeMillis());
    uuidAndTime.put(String.valueOf(uuid.incrementAndGet()), System.currentTimeMillis());
    uuidAndTime.put(String.valueOf(uuid.incrementAndGet()), System.currentTimeMillis());
    final int i1 = uuid.incrementAndGet();
    uuidAndTime.put(String.valueOf(i1), System.currentTimeMillis());
    Map<String, Long> appAndTime = applicationManager.getQuotaManager().getCurrentUserAndApp()
        .computeIfAbsent("user1", x -> uuidAndTime);
    // This thread may remove the uuid and put the appId in.
    final Thread registerThread = new Thread(() ->
        applicationManager.getQuotaManager().registerApplicationInfo("application_test_" + i1, appAndTime));
    registerThread.start();
    final boolean icCheck = applicationManager.getQuotaManager()
        .checkQuota("user1", String.valueOf(i1));
    registerThread.join();
    assertTrue(icCheck);
    assertEquals(applicationManager.getQuotaManager().getCurrentUserAndApp().get("user1").size(), 5);
  }
}
