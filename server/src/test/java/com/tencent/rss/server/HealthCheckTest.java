/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
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

package com.tencent.rss.server;

import com.tencent.rss.storage.util.StorageType;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HealthCheckTest {

  @Test
  public void constructorTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    assertConf(conf);
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES, "");
    assertConf(conf);
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES, "com.tencent.rss.server.LocalStorageChecker");
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "s1");
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    assertConf(conf);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE, 102.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE, 1.0);
    conf.set(ShuffleServerConf.HEALTH_CHECK_INTERVAL, -1L);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_CHECK_INTERVAL, 1L);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, 101.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, 1.0);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, 101.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, 1.0);
    new HealthCheck(new AtomicBoolean(), conf);
  }

  @Test
  public void checkTest() {
    AtomicBoolean healthy = new AtomicBoolean(false);
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES, HealthyMockChecker.class.getCanonicalName());
    HealthCheck checker = new HealthCheck(healthy, conf);
    checker.check();
    assertTrue(healthy.get());
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES, UnHealthyMockChecker.class.getCanonicalName());
    checker = new HealthCheck(healthy, conf);
    checker.check();
    assertFalse(healthy.get());
    conf.setString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES,
        UnHealthyMockChecker.class.getCanonicalName() + "," + HealthyMockChecker.class.getCanonicalName());
    checker = new HealthCheck(healthy, conf);
    checker.check();
    assertFalse(healthy.get());
  }

  private void assertConf(ShuffleServerConf conf) {
    boolean isThrown;
    isThrown = false;
    try {
      new HealthCheck(new AtomicBoolean(), conf);
    } catch (IllegalArgumentException e) {
      isThrown = true;
    }
    assertTrue(isThrown);
  }
}
