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

package org.apache.uniffle.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import scala.Option;

import com.google.common.collect.Maps;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssShuffleManagerTest extends SparkIntegrationTestBase {

  @BeforeAll
  public static void setupServers() throws Exception {
    shutdownServers();
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(
        RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    // we don't need to run any spark application here, just return an empty map.
    return new HashMap();
  }

  @Test
  public void testRssShuffleManager() throws Exception {
    SparkConf conf = createSparkConf();
    updateSparkConfWithRss(conf);
    // enable stage recompute
    conf.set("spark." + RssClientConfig.RSS_RESUBMIT_STAGE, "true");
    // disable remote storage fetch
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED, false);
    conf.set("spark." + RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE.name());

    JavaSparkContext sc = null;
    try {
      Option<SparkSession> spark = SparkSession.getActiveSession();
      if (spark.nonEmpty()) {
        spark.get().stop();
      }
      sc = new JavaSparkContext(SparkSession.builder().config(conf).getOrCreate().sparkContext());
      // create a rdd that triggers shuffle registration
      long count = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2).groupBy(x -> x).count();
      assertEquals(5, count);
      RssShuffleManager shuffleManager = (RssShuffleManager) SparkEnv.get().shuffleManager();
      shuffleManager.unregisterAllMapOutput(0);
      MapOutputTrackerMaster master = (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();
      assertTrue(master.containsShuffle(0));
    } finally {
      if (sc != null) {
        sc.stop();
      }
    }
  }
}
