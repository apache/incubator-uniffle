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
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamicFetchClientConfTest extends IntegrationTestBase {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set(RssClientConfig.RSS_COORDINATOR_QUORUM, COORDINATOR_QUORUM);
    sparkConf.set("spark.mock.2", "no-overwrite-conf");
    sparkConf.set("spark.shuffle.service.enabled", "true");

    String cfgFile = HDFS_URI + "/test/client_conf";
    Path path = new Path(cfgFile);
    FSDataOutputStream out = fs.create(path);
    PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.1  1234");
    printWriter.println(" spark.mock.2 overwrite-conf ");
    printWriter.println(" spark.mock.3 true ");
    printWriter.println("spark.rss.storage.type " + StorageType.MEMORY_LOCALFILE_HDFS.name());
    printWriter.println("spark.rss.base.path expectedPath");
    printWriter.flush();
    printWriter.close();
    for (String k : RssClientConfig.RSS_MANDATORY_CLUSTER_CONF) {
      sparkConf.set(k, "Dummy-" + k);
    }
    sparkConf.set("spark.mock.2", "no-overwrite-conf");

    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setBoolean("rss.coordinator.dynamicClientConf.enabled", true);
    coordinatorConf.setString("rss.coordinator.dynamicClientConf.path", cfgFile);
    coordinatorConf.setInteger("rss.coordinator.dynamicClientConf.updateIntervalSec", 10);
    createCoordinatorServer(coordinatorConf);
    startServers();

    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    assertFalse(sparkConf.contains("spark.mock.1"));
    assertEquals("no-overwrite-conf", sparkConf.get("spark.mock.2"));
    assertFalse(sparkConf.contains("spark.mock.3"));
    assertEquals("Dummy-spark.rss.storage.type", sparkConf.get("spark.rss.storage.type"));
    assertEquals("Dummy-spark.rss.base.path", sparkConf.get("spark.rss.base.path"));
    assertTrue(sparkConf.getBoolean("spark.shuffle.service.enabled", true));

    RssShuffleManager rssShuffleManager = new RssShuffleManager(sparkConf, true);

    SparkConf sparkConf1 = rssShuffleManager.getSparkConf();
    assertEquals(1234, sparkConf1.getInt("spark.mock.1", 0));
    assertEquals("no-overwrite-conf", sparkConf1.get("spark.mock.2"));
    assertEquals(StorageType.MEMORY_LOCALFILE_HDFS.name(), sparkConf.get("spark.rss.storage.type"));
    assertEquals("expectedPath", sparkConf.get("spark.rss.base.path"));
    assertFalse(sparkConf1.getBoolean("spark.shuffle.service.enabled", true));

    fs.delete(path, true);
    shutdownServers();
    sparkConf = new SparkConf();
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set(RssClientConfig.RSS_COORDINATOR_QUORUM, COORDINATOR_QUORUM);
    sparkConf.set("spark.mock.2", "no-overwrite-conf");
    sparkConf.set("spark.shuffle.service.enabled", "true");

    out = fs.create(path);
    printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.1  1234");
    printWriter.println(" spark.mock.2 overwrite-conf ");
    printWriter.println(" spark.mock.3 true ");
    printWriter.flush();
    printWriter.close();
    coordinatorConf = getCoordinatorConf();
    coordinatorConf.setBoolean("rss.coordinator.dynamicClientConf.enabled", true);
    coordinatorConf.setString("rss.coordinator.dynamicClientConf.path", cfgFile);
    coordinatorConf.setInteger("rss.coordinator.dynamicClientConf.updateIntervalSec", 10);
    createCoordinatorServer(coordinatorConf);
    startServers();
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    Exception expectException = null;
    try {
      new RssShuffleManager(sparkConf, true);
    } catch (IllegalArgumentException e) {
      expectException = e;
    }
    assertEquals(
        "Storage type must be set by the client or fetched from coordinators.",
        expectException.getMessage());
  }
}
