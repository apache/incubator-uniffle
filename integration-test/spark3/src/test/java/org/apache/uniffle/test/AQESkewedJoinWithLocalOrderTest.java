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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.BeforeAll;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

public class AQESkewedJoinWithLocalOrderTest extends AQESkewedJoinTest {

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    // Use the LOCALFILE storage type to ensure the data will be flushed by local_order mechanism
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_STORAGE_TYPE.key(), "LOCALFILE");
    sparkConf.set("spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.LOCAL_ORDER.name());
  }
}
