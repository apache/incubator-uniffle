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

package org.apache.spark.shuffle;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RssSparkShuffleUtilsTest {
  @Test
  public void odfsConfigurationTest() {
    SparkConf conf = new SparkConf();
    Configuration conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertFalse(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.fs.Hdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(RssClientConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE, "true");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertTrue(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfsFilesystem", conf1.get("fs.hdfs.impl"));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(RssClientConfig.RSS_OZONE_FS_HDFS_IMPL, "expect_odfs_impl");
    conf.set(RssClientConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL, "expect_odfs_abstract_impl");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertEquals("expect_odfs_impl", conf1.get("fs.hdfs.impl"));
    assertEquals("expect_odfs_abstract_impl", conf1.get("fs.AbstractFileSystem.hdfs.impl"));
  }

  @Test
  public void applyDynamicClientConfTest() {
    SparkConf conf = new SparkConf();
    Map<String, String> clientConf = Maps.newHashMap();
    String remoteStoragePath = "hdfs://path1";
    String mockKey = "spark.mockKey";
    String mockValue = "v";
    clientConf.put(RssClientConfig.RSS_BASE_PATH, remoteStoragePath);
    clientConf.put(mockKey, mockValue);
    RssSparkShuffleUtils.applyDynamicClientConf(conf, clientConf);
    assertEquals(remoteStoragePath, conf.get(RssClientConfig.RSS_BASE_PATH));
    assertEquals(mockValue, conf.get(mockKey));

    String remoteStoragePath2 = "hdfs://path2";
    clientConf = Maps.newHashMap();
    clientConf.put(RssClientConfig.RSS_BASE_PATH, remoteStoragePath2);
    clientConf.put(mockKey, "won't be rewrite");
    RssSparkShuffleUtils.applyDynamicClientConf(conf, clientConf);
    assertEquals(remoteStoragePath2, conf.get(RssClientConfig.RSS_BASE_PATH));
    assertEquals(mockValue, conf.get(mockKey));
  }

}
