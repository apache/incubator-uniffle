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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.rpc.StatusCode.ACCESS_DENIED;
import static org.apache.uniffle.common.rpc.StatusCode.SUCCESS;
import static org.junit.jupiter.api.Assertions.*;

public class HybridShuffleManagerTest extends RssShuffleManagerTestBase {

  @Test
  public void testCreateInDriverDenied() throws Exception {
    setupMockedRssShuffleUtils(ACCESS_DENIED);
    setupMockedShuffleManager();
    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    assertCreateSortShuffleManager(conf, true);
  }

  @Test
  public void testCreateInDriver() throws Exception {
    setupMockedRssShuffleUtils(SUCCESS);
    setupMockedShuffleManager();
    SparkConf conf = new SparkConf();
    assertCreateSortShuffleManager(conf, true);
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_ACCESS_ID.key(), "mockId");
    assertCreateSortShuffleManager(conf, true);
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    assertCreateRssShuffleManager(conf);

    conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    assertCreateSortShuffleManager(conf, true);
  }

  @Test
  public void testCreateInExecutor() throws Exception {
    DelegationRssShuffleManager delegationRssShuffleManager;
    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    assertCreateSortShuffleManager(conf, false);
  }

  private void assertCreateSortShuffleManager(SparkConf conf, boolean isDriver) throws Exception {
    HybridShuffleManager hybridShuffleManager = new HybridShuffleManager(conf, true);
    assertNull(hybridShuffleManager.getEssShuffleManager());
    assertNull(hybridShuffleManager.getRssShuffleManager());
    hybridShuffleManager.registerShuffleManager(0);
    assertTrue(hybridShuffleManager.getEssShuffleManager() instanceof SortShuffleManager);
    assertNotNull(hybridShuffleManager.getEssShuffleManager());
    assertNull(hybridShuffleManager.getRssShuffleManager());
    assertFalse(conf.getBoolean(RssSparkConfig.RSS_ENABLED.key(), false));
    assertEquals("sort", conf.get("spark.shuffle.manager"));
  }

  private void assertCreateRssShuffleManager(SparkConf conf) throws Exception {
    HybridShuffleManager hybridShuffleManager = new HybridShuffleManager(conf, true);
    assertNull(hybridShuffleManager.getEssShuffleManager());
    assertNull(hybridShuffleManager.getRssShuffleManager());
    hybridShuffleManager.registerShuffleManager(0);
    assertTrue(hybridShuffleManager.getRssShuffleManager() instanceof RssShuffleManager);
    assertNotNull(hybridShuffleManager.getRssShuffleManager());
    assertNull(hybridShuffleManager.getEssShuffleManager());
    assertTrue(Boolean.parseBoolean(conf.get(RssSparkConfig.RSS_ENABLED.key())));
    assertEquals(RssShuffleManager.class.getCanonicalName(), conf.get("spark.shuffle.manager"));
  }
}
