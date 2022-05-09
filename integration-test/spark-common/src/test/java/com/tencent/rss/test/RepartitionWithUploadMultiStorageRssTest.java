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

public class RepartitionWithUploadMultiStorageRssTest extends RepartitionTest {
  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE, StorageType.LOCALFILE_HDFS_2.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();

    // local storage config
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE_HDFS_2.name());

    // uploader and remote storage config
    shuffleServerConf.setBoolean("rss.server.uploader.enable", true);
    shuffleServerConf.setLong("rss.server.uploader.combine.threshold.MB", 32);
    shuffleServerConf.setLong("rss.server.uploader.references.speed.mbps", 128);
    shuffleServerConf.setString("rss.server.uploader.remote.storage.type", StorageType.HDFS.name());
    shuffleServerConf.setString("rss.server.uploader.base.path", HDFS_URI + "rss/test");
    shuffleServerConf.setLong("rss.server.uploader.interval.ms", 10);
    shuffleServerConf.setInteger("rss.server.uploader.thread.number", 4);

    shuffleServerConf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 1024L * 1024L);

    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
  }
}
