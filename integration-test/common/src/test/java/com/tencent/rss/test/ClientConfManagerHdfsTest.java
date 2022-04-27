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

package com.tencent.rss.test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.tencent.rss.coordinator.ApplicationManager;
import com.tencent.rss.coordinator.ClientConfManager;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.storage.HdfsTestBase;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ClientConfManagerHdfsTest extends HdfsTestBase {

  @Test
  public void test() throws Exception {
    String cfgFile = HDFS_URI + "/test/client_conf";
    Path path = new Path(cfgFile);
    FSDataOutputStream out = fs.create(path);

    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, HDFS_URI);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, 1);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);

    // file load checking at startup
    Exception expectedException = null;
    try {
      new ClientConfManager(conf, new Configuration(), new ApplicationManager(conf));
    } catch (RuntimeException e) {
      expectedException = e;
    }
    assertNotNull(expectedException);
    assertTrue(expectedException.getMessage().endsWith("is not a file."));

    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile);
    ClientConfManager clientConfManager = new ClientConfManager(
        conf, new Configuration(), new ApplicationManager(conf));
    assertEquals(0, clientConfManager.getClientConf().size());

    PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.1 abc");
    printWriter.println(" spark.mock.2   123 ");
    printWriter.println("spark.mock.3 true  ");
    printWriter.flush();
    printWriter.close();
    clientConfManager = new ClientConfManager(conf, HdfsTestBase.conf, new ApplicationManager(conf));
    sleep(1200);
    Map<String, String> clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // ignore empty or wrong content
    printWriter.println("");
    printWriter.flush();
    printWriter.close();
    sleep(1300);
    assertTrue(fs.exists(path));
    clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // the config will not be changed when the conf file is deleted
    fs.delete(path, true);
    assertFalse(fs.exists(path));
    sleep(1200);
    clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    // the normal update config process, move the new conf file to the old one
    Path tmpPath = new Path(cfgFile + ".tmp");
    out = fs.create(tmpPath);
    printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.4 deadbeaf");
    printWriter.println("spark.mock.5 9527");
    printWriter.println("spark.mock.6 9527 3423");
    printWriter.println("spark.mock.7");
    printWriter.close();
    fs.rename(tmpPath, path);
    sleep(1200);
    clientConf = clientConfManager.getClientConf();
    assertEquals("deadbeaf", clientConf.get("spark.mock.4"));
    assertEquals("9527", clientConf.get("spark.mock.5"));
    assertEquals(2, clientConf.size());
    assertFalse(clientConf.containsKey("spark.mock.6"));
    assertFalse(clientConf.containsKey("spark.mock.7"));
    clientConfManager.close();
  }
}
