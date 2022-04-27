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

import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

public class RepartitionWithMemoryRssTest extends RepartitionTest {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.set(CoordinatorConf.COORDINATOR_APP_EXPIRED, 5000L);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.set(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 4000L);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
    shuffleServerConf.setString(ShuffleServerConf.SERVER_BUFFER_CAPACITY.key(), "512mb");
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Test
  public void testMemoryRelease() throws Exception {
    String fileName = generateTextFile(10000, 10000);
    SparkConf sparkConf = createSparkConf();
    updateSparkConfWithRss(sparkConf);
    sparkConf.set("spark.executor.memory", "500m");
    sparkConf.set("spark.unsafe.exceptionOnMemoryLeak", "true");
    updateRssStorage(sparkConf);

    // oom if there has no memory release
    runSparkApp(sparkConf, fileName);
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE.name());
  }
}
