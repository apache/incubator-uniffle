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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.tencent.rss.common.util.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ClientConfManagerTest {
  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @Test
  public void test() throws Exception {
    File cfgFile = tmpDir.newFile();
    String cfgFileName = cfgFile.getAbsolutePath();
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, tmpDir.getRoot().toURI().toString());
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    ApplicationManager applicationManager = new ApplicationManager(conf);

    // file load checking at startup
    Exception expectedException = null;
    try {
      new ClientConfManager(conf, new Configuration(), applicationManager);
    } catch (RuntimeException e) {
      expectedException = e;
    }
    assertNotNull(expectedException);
    assertTrue(expectedException.getMessage().endsWith("is not a file."));

    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    ClientConfManager clientConfManager = new ClientConfManager(conf, new Configuration(), applicationManager);
    assertEquals(0, clientConfManager.getClientConf().size());

    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.1 abc");
    printWriter.println(" spark.mock.2   123 ");
    printWriter.println("spark.mock.3 true  ");
    printWriter.flush();
    printWriter.close();
    // load config at the beginning
    clientConfManager = new ClientConfManager(conf, new Configuration(), applicationManager);
    Thread.sleep(1200);
    Map<String, String> clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // ignore empty or wrong content
    printWriter.println("");
    printWriter.flush();
    printWriter.close();
    Thread.sleep(1300);
    assertTrue(cfgFile.exists());
    clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // the config will not be changed when the conf file is deleted
    assertTrue(cfgFile.delete());
    Thread.sleep(1300);
    assertFalse(cfgFile.exists());
    clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // the normal update config process, move the new conf file to the old one
    File cfgFileTmp = new File(cfgFileName + ".tmp");
    fileWriter = new FileWriter(cfgFileTmp);
    printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.4 deadbeaf");
    printWriter.println("spark.mock.5 9527");
    printWriter.println("spark.mock.6 9527 3423");
    printWriter.println("spark.mock.7");
    printWriter.close();
    FileUtils.moveFile(cfgFileTmp, cfgFile);
    Thread.sleep(1200);
    clientConf = clientConfManager.getClientConf();
    assertEquals("deadbeaf", clientConf.get("spark.mock.4"));
    assertEquals("9527", clientConf.get("spark.mock.5"));
    assertEquals(2, clientConf.size());
    assertFalse(clientConf.containsKey("spark.mock.6"));
    assertFalse(clientConf.containsKey("spark.mock.7"));
    clientConfManager.close();
  }

  @Test
  public void dynamicRemoteStorageTest() throws Exception {
    int updateIntervalSec = 2;
    String remotePath1 = "hdfs://path1";
    String remotePath2 = "hdfs://path2";
    String remotePath3 = "hdfs://path3";
    File cfgFile = Files.createTempFile("dynamicConf", ".conf").toFile();
    writeRemoteStorageConf(cfgFile, remotePath1);

    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, updateIntervalSec);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    ApplicationManager applicationManager = new ApplicationManager(conf);

    ClientConfManager clientConfManager = new ClientConfManager(conf, new Configuration(), applicationManager);
    Thread.sleep(500);
    Set<String> expectedAvailablePath = Sets.newHashSet(remotePath1);
    assertEquals(expectedAvailablePath, applicationManager.getAvailableRemoteStoragePath());
    assertEquals(remotePath1, applicationManager.pickRemoteStoragePath("testAppId1"));

    writeRemoteStorageConf(cfgFile, remotePath3);
    expectedAvailablePath = Sets.newHashSet(remotePath3);
    waitForUpdate(expectedAvailablePath, applicationManager);
    assertEquals(remotePath3, applicationManager.pickRemoteStoragePath("testAppId2"));

    writeRemoteStorageConf(cfgFile, remotePath2 + Constants.COMMA_SPLIT_CHAR + remotePath3);
    expectedAvailablePath = Sets.newHashSet(remotePath2, remotePath3);
    waitForUpdate(expectedAvailablePath, applicationManager);
    assertEquals(remotePath2, applicationManager.pickRemoteStoragePath("testAppId3"));

    writeRemoteStorageConf(cfgFile, remotePath1 + Constants.COMMA_SPLIT_CHAR + remotePath2);
    expectedAvailablePath = Sets.newHashSet(remotePath1, remotePath2);
    waitForUpdate(expectedAvailablePath, applicationManager);
    String remoteStorage = applicationManager.pickRemoteStoragePath("testAppId4");
    // one of remote storage will be chosen
    assertTrue(remotePath1.equals(remoteStorage) || remotePath2.equals(remoteStorage));

    clientConfManager.close();
  }

  private void writeRemoteStorageConf(File cfgFile, String value) throws Exception {
    // sleep 2 secs to make sure the modified time will be updated
    Thread.sleep(2000);
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key() + " " + value);
    printWriter.flush();
    printWriter.close();
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
