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

package org.apache.uniffle.coordinator.conf;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicClientConfServiceTest {

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void testByLegacyParser(@TempDir File tempDir) throws Exception {
    File cfgFile = File.createTempFile("tmp", ".conf", tempDir);
    final String cfgFileName = cfgFile.getAbsolutePath();
    final String filePath =
        Objects.requireNonNull(getClass().getClassLoader().getResource("coordinator.conf"))
            .getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, tempDir.toURI().toString());
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);

    // file load checking at startup
    Exception expectedException = null;
    DynamicClientConfService dynamicClientConfService = null;
    try {
      dynamicClientConfService = new DynamicClientConfService(conf, new Configuration());
    } catch (RuntimeException e) {
      expectedException = e;
    } finally {
      if (dynamicClientConfService != null) {
        dynamicClientConfService.close();
      }
    }
    assertNotNull(expectedException);
    assertTrue(expectedException.getMessage().endsWith("is not a file."));

    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    DynamicClientConfService clientConfManager =
        new DynamicClientConfService(conf, new Configuration());
    assertEquals(0, clientConfManager.getRssClientConf().size());

    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.1 abc");
    printWriter.println(" spark.mock.2   123 ");
    printWriter.println("spark.mock.3 true  ");
    printWriter.flush();
    printWriter.close();
    // load config at the beginning
    clientConfManager = new DynamicClientConfService(conf, new Configuration());
    Thread.sleep(1200);
    Map<String, String> clientConf = clientConfManager.getRssClientConf();
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
    clientConf = clientConfManager.getRssClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // the config will not be changed when the conf file is deleted
    assertTrue(cfgFile.delete());
    Thread.sleep(1300);
    assertFalse(cfgFile.exists());
    clientConf = clientConfManager.getRssClientConf();
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
    clientConf = clientConfManager.getRssClientConf();
    assertEquals("deadbeaf", clientConf.get("spark.mock.4"));
    assertEquals("9527", clientConf.get("spark.mock.5"));
    assertEquals(2, clientConf.size());
    assertFalse(clientConf.containsKey("spark.mock.6"));
    assertFalse(clientConf.containsKey("spark.mock.7"));
    clientConfManager.close();
  }
}
