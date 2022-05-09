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

import java.io.File;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.BeforeClass;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;

public class SparkSQLWithMemoryLocalTest extends SparkSQLTest {

  private static String basePath;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
  }

  @Override
  public void checkShuffleData() throws Exception {
    Thread.sleep(12000);
    String[] paths = basePath.split(",");
    for (String path : paths) {
      File f = new File(path);
      assertEquals(0, f.list().length);
    }
  }
}
