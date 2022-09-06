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

package org.apache.spark.shuffle;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.Constants;

import static org.apache.spark.shuffle.RssSparkClientConf.SPARK_CONFIG_KEY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssSparkShuffleUtilsTest {

  @Test
  public void testAssignmentTags() {
    RssConf conf = new RssConf();

    /**
     * Case1: dont set the tag implicitly and will return the {@code Constants.SHUFFLE_SERVER_VERSION}
      */
    Set<String> tags = RssSparkShuffleUtils.getAssignmentTags(conf);
    assertEquals(Constants.SHUFFLE_SERVER_VERSION, tags.iterator().next());

    /**
     * Case2: set the multiple tags implicitly and will return the {@code Constants.SHUFFLE_SERVER_VERSION}
     * and configured tags.
     */
    conf.set(RssSparkClientConf.RSS_CLIENT_ASSIGNMENT_TAGS, Arrays.asList("a", "b"));
    tags = RssSparkShuffleUtils.getAssignmentTags(conf);
    assertEquals(3, tags.size());
    Iterator<String> iterator = tags.iterator();
    assertEquals("a", iterator.next());
    assertEquals("b", iterator.next());
    assertEquals(Constants.SHUFFLE_SERVER_VERSION, iterator.next());
  }

  @Test
  public void odfsConfigurationTest() {
    SparkConf conf = new SparkConf();
    Configuration conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertFalse(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.fs.Hdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.key(), "true");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertTrue(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfsFilesystem", conf1.get("fs.hdfs.impl"));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_FS_HDFS_IMPL.key(), "expect_odfs_impl");
    conf.set(SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL.key(),
        "expect_odfs_abstract_impl");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertEquals("expect_odfs_impl", conf1.get("fs.hdfs.impl"));
    assertEquals("expect_odfs_abstract_impl", conf1.get("fs.AbstractFileSystem.hdfs.impl"));
  }
}
