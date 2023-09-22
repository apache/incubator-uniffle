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

import java.io.File;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthScriptCheckerTest {
  private String healthScriptTemplate1;
  private String healthScriptTemplate2;
  private String healthScriptTemplate3;
  private String healthScriptTemplate4;

  @BeforeEach
  void setUp() {
    healthScriptTemplate1 = getScriptFilePath("healthy-script1.sh");
    healthScriptTemplate2 = getScriptFilePath("healthy-script2.sh");
    healthScriptTemplate3 = getScriptFilePath("healthy-script3.sh");
    healthScriptTemplate4 = getScriptFilePath("healthy-script4.sh");
    setExecutable(healthScriptTemplate1);
    setExecutable(healthScriptTemplate2);
    setExecutable(healthScriptTemplate3);
    setExecutable(healthScriptTemplate4);
  }

  @Test
  void checkIsHealthy() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_PATH, healthScriptTemplate1);
    HealthScriptChecker checker = new HealthScriptChecker(conf);
    assertTrue(checker.checkIsHealthy());

    conf.setString(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_PATH, healthScriptTemplate2);
    checker = new HealthScriptChecker(conf);
    assertFalse(checker.checkIsHealthy());

    conf.setString(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_PATH, healthScriptTemplate3);
    checker = new HealthScriptChecker(conf);
    assertFalse(checker.checkIsHealthy());

    conf.setString(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_PATH, healthScriptTemplate4);
    conf.setLong(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_EXECUTE_TIMEOUT, 3000L);
    checker = new HealthScriptChecker(conf);
    assertFalse(checker.checkIsHealthy());
  }

  private String getScriptFilePath(String fileName) {
    return Objects.requireNonNull(this.getClass().getClassLoader().getResource(fileName)).getFile();
  }

  private boolean setExecutable(String filePath) {
    File file = new File(filePath);
    file.setExecutable(true);
    return file.canExecute();
  }
}
