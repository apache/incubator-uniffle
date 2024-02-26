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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Option;

import com.google.common.collect.Maps;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleHandleInfo;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
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
    createShuffleServer(getShuffleServerConf(ServerType.GRPC));
    startServers();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    // we don't need to run any spark application here, just return an empty map.
    return new HashMap();
  }

  @Test
  public void testRssShuffleManager() throws Exception {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

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
      RssShuffleManagerBase shuffleManager =
          (RssShuffleManagerBase) SparkEnv.get().shuffleManager();

      // get written block ids (we know there is one shuffle where two task attempts wrote two
      // partitions)
      RssConf rssConf = RssSparkConfig.toRssConf(conf);
      ShuffleWriteClient shuffleWriteClient =
          ShuffleClientFactory.newWriteBuilder()
              .clientType(ClientType.GRPC.name())
              .retryMax(3)
              .retryIntervalMax(2000)
              .heartBeatThreadNum(4)
              .replica(1)
              .replicaWrite(1)
              .replicaRead(1)
              .replicaSkipEnabled(true)
              .dataTransferPoolSize(1)
              .dataCommitPoolSize(1)
              .unregisterThreadPoolSize(10)
              .unregisterRequestTimeSec(10)
              .rssConf(rssConf)
              .build();
      ShuffleHandleInfo handle = shuffleManager.getShuffleHandleInfoByShuffleId(0);
      Set<ShuffleServerInfo> servers =
          handle.getPartitionToServers().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());

      for (int partitionId : new int[] {0, 1}) {
        Roaring64NavigableMap blockIdLongs =
            shuffleWriteClient.getShuffleResult(
                ClientType.GRPC.name(), servers, shuffleManager.getAppId(), 0, partitionId);
        List<BlockId> blockIds =
            blockIdLongs.stream().sorted().mapToObj(layout::asBlockId).collect(Collectors.toList());
        assertEquals(2, blockIds.size());
        long taskAttemptId0 = shuffleManager.getTaskAttemptId(0, 0, 0);
        long taskAttemptId1 = shuffleManager.getTaskAttemptId(1, 0, 1);
        assertEquals(
            layout.asBlockId(0, partitionId, taskAttemptId0), blockIds.get(0), layout.toString());
        assertEquals(
            layout.asBlockId(0, partitionId, taskAttemptId1), blockIds.get(1), layout.toString());
      }

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
