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
import java.util.ArrayList;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.junit.jupiter.api.BeforeAll;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

public class StatefulUpgradeTest extends SparkSQLTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatefulUpgradeTest.class);

  private static File shuffleDataFile;
  private static ShuffleServer shuffleServer;

  private static boolean allowRead = false;
  private static boolean readPoint = false;

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    shuffleDataFile = new File(tmpDir, "data1");
    shuffleServerConf.setString("rss.storage.basePath", shuffleDataFile.getAbsolutePath());

    shuffleServerConf.setBoolean("rss.server.stateful.upgrade.enable", true);
    shuffleServerConf.setString(
        "rss.server.stateful.upgrade.state.export.location",
        shuffleDataFile.getAbsolutePath() + "/state.bin"
    );

    createShuffleServer(shuffleServerConf);
    startServers();

    shuffleServer = shuffleServers.get(0);

    Thread thread = new Thread(() -> {
      try {
        while (true) {
          if (readPoint) {
            shuffleServer.getStatefulUpgradeManager().finalizeAndMaterializeState();
            shuffleServer.stopServer();
            shuffleServers = new ArrayList<>();
            Thread.sleep(1000 * 5);
            shuffleServers.add(new ShuffleServer(shuffleServerConf, true));
            shuffleServers.get(0).start();
            shuffleServer = shuffleServers.get(0);
            allowRead = true;
            LOGGER.info("Finished stateful restarting shuffle-server");
            return;
          }
          Thread.sleep(100);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to running restarting operation", e);
      }
    });
    thread.start();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set("spark.shuffle.manager", "org.apache.uniffle.test.StatefulUpgradeTest$MockRssShuffleManager");
    sparkConf.set("spark.rss.client.stateful.upgrade.enable", "true");
  }

  @Override
  public void checkShuffleData() throws Exception {

  }

  public static class MockRssShuffleManager extends RssShuffleManager {

    public MockRssShuffleManager(SparkConf conf, boolean isDriver) {
      super(conf, isDriver);
    }

    public <K, C> ShuffleReader<K, C> getReaderImpl(
        ShuffleHandle handle,
        int startMapIndex,
        int endMapIndex,
        int startPartition,
        int endPartition,
        TaskContext context,
        ShuffleReadMetricsReporter metrics,
        Roaring64NavigableMap taskIdBitmap) {
      readPoint = true;
      try {
        while (true) {
          Thread.sleep(1000);
          if (allowRead) {
            LOGGER.info("Shuffle server has been restarted.");
            break;
          }
        }
      } catch (Exception e) {
        // ignore
      }
      return super.getReaderImpl(handle, startMapIndex, endMapIndex, startPartition, endPartition,
          context, metrics, taskIdBitmap);
    }
  }
}
