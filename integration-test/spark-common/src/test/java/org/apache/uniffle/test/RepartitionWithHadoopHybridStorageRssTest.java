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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.spark.shuffle.RssSparkConfig;
import org.apache.uniffle.storage.util.StorageType;

public class RepartitionWithHadoopHybridStorageRssTest extends RepartitionTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(RepartitionWithHadoopHybridStorageRssTest.class);

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE_HDFS.name());
    Random random = new Random();
    // todo: we should use parameterized test to modify here when we could solve the issue that
    //  the test case use too long time.
    boolean useOffHeap = random.nextInt() % 2 == 0;
    LOG.info("use off heap: " + useOffHeap);
    dynamicConf.put(
        RssSparkConfig.RSS_CLIENT_OFF_HEAP_MEMORY_ENABLE.key(), String.valueOf(useOffHeap));
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    // local storage config
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    List<String> grpcBasePath =
        Arrays.asList(dataDir1.getAbsolutePath(), dataDir2.getAbsolutePath());
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, grpcBasePath);
    createShuffleServer(grpcShuffleServerConf);

    // local storage config
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    List<String> nettyBasePath =
        Arrays.asList(dataDir3.getAbsolutePath(), dataDir4.getAbsolutePath());
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, nettyBasePath);
    createShuffleServer(nettyShuffleServerConf);

    startServers();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      ServerType serverType, List<String> basePath) throws Exception {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE_HDFS.name());
    shuffleServerConf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 1024L * 1024L);
    return shuffleServerConf;
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {}
}
