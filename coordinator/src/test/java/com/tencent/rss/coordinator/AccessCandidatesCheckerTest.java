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
import java.util.Objects;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AccessCandidatesCheckerTest {
  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @After
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void test() throws Exception {
    File cfgFile = tmpDir.newFile();
    String cfgFileName = cfgFile.getAbsolutePath();
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.set(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH, tmpDir.getRoot().toURI().toString());
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        "com.tencent.rss.coordinator.AccessCandidatesChecker");

    // file load checking at startup
    Exception expectedException = null;
    try {
      new AccessManager(conf, null, new Configuration());
    } catch (RuntimeException e) {
      expectedException = e;
    }
    assertNotNull(expectedException);
    assertTrue(expectedException.getMessage().contains(
        "NoSuchMethodException: com.tencent.rss.coordinator.AccessCandidatesChecker.<init>()"));
    conf.set(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH, cfgFile.toURI().toString());
    expectedException = null;
    try {
      new AccessManager(conf, null, new Configuration());
    } catch (RuntimeException e) {
      expectedException = e;
    }
    assertNotNull(expectedException);
    assertTrue(expectedException.getMessage().contains(
        "NoSuchMethodException: com.tencent.rss.coordinator.AccessCandidatesChecker.<init>()"));

    // load the config at the beginning
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("9527");
    printWriter.println(" 135 ");
    printWriter.println("2 ");
    printWriter.flush();
    printWriter.close();
    AccessManager accessManager = new AccessManager(conf, null, new Configuration());
    AccessCandidatesChecker checker = (AccessCandidatesChecker) accessManager.getAccessCheckers().get(0);
    sleep(1200);
    assertEquals(Sets.newHashSet("2", "9527", "135"), checker.getCandidates().get());
    assertTrue(checker.check(new AccessInfo("9527")).isSuccess());
    assertTrue(checker.check(new AccessInfo("135")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1_2")).isSuccess());

    // ignore empty or wrong content
    printWriter.println("");
    printWriter.flush();
    printWriter.close();
    sleep(1300);
    assertTrue(cfgFile.exists());
    assertEquals(Sets.newHashSet("2", "9527", "135"), checker.getCandidates().get());
    assertTrue(checker.check(new AccessInfo("9527")).isSuccess());
    assertTrue(checker.check(new AccessInfo("135")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1_2")).isSuccess());

    // the config will not be changed when the conf file is deleted
    assertTrue(cfgFile.delete());
    sleep(1200);
    assertEquals(Sets.newHashSet("2", "9527", "135"), checker.getCandidates().get());
    assertTrue(checker.check(new AccessInfo("9527")).isSuccess());
    assertTrue(checker.check(new AccessInfo("135")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1")).isSuccess());
    assertFalse(checker.check(new AccessInfo("1_2")).isSuccess());

    // the normal update config process, move the new conf file to the old one
    File cfgFileTmp = new File(cfgFileName + ".tmp");
    fileWriter = new FileWriter(cfgFileTmp);
    printWriter = new PrintWriter(fileWriter);
    printWriter.println("13");
    printWriter.println("57");
    printWriter.close();
    FileUtils.moveFile(cfgFileTmp, cfgFile);
    sleep(1200);
    assertEquals(Sets.newHashSet("13", "57"), checker.getCandidates().get());
    assertTrue(checker.check(new AccessInfo("13")).isSuccess());
    assertTrue(checker.check(new AccessInfo("57")).isSuccess());
    checker.close();
  }
}
