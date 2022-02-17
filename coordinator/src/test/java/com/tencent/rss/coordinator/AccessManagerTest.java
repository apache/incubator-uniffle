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

package com.tencent.rss.coordinator;

import com.google.common.collect.Sets;
import com.tencent.rss.common.util.Constants;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccessManagerTest {
  @Before
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @Test
  public void test() throws Exception {
    // test init
    CoordinatorConf conf = new CoordinatorConf();
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS, " , ");
    try {
      new AccessManager(conf, null, new Configuration());
    } catch (RuntimeException e) {
      String expectedMessage = "Empty classes";
      assertTrue(e.getMessage().startsWith(expectedMessage));
    }
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        "com.Dummy,com.tencent.rss.coordinator.AccessManagerTest$MockAccessChecker");
    try {
      new AccessManager(conf, null, new Configuration());
    } catch (RuntimeException e) {
      String expectedMessage = "java.lang.ClassNotFoundException: com.Dummy";
      assertTrue(e.getMessage().startsWith(expectedMessage));
    }
    // test empty checkers
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS, "");
    AccessManager accessManager = new AccessManager(conf, null, new Configuration());
    assertTrue(accessManager.handleAccessRequest(
            new AccessInfo(String.valueOf(new Random().nextInt()),
                Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION)))
        .isSuccess());
    accessManager.close();
    // test mock checkers
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        "com.tencent.rss.coordinator.AccessManagerTest$MockAccessCheckerAlwaysTrue,");
    accessManager = new AccessManager(conf, null, new Configuration());
    assertEquals(1, accessManager.getAccessCheckers().size());
    assertTrue(accessManager.handleAccessRequest(new AccessInfo("mock1")).isSuccess());
    assertTrue(accessManager.handleAccessRequest(new AccessInfo("mock2")).isSuccess());
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        "com.tencent.rss.coordinator.AccessManagerTest$MockAccessCheckerAlwaysTrue,"
            + "com.tencent.rss.coordinator.AccessManagerTest$MockAccessCheckerAlwaysFalse");
    accessManager = new AccessManager(conf, null, new Configuration());
    assertEquals(2, accessManager.getAccessCheckers().size());
    assertFalse(accessManager.handleAccessRequest(new AccessInfo("mock1")).isSuccess());
    accessManager.close();
  }

  public static class MockAccessCheckerAlwaysTrue implements AccessChecker {
    public MockAccessCheckerAlwaysTrue(AccessManager accessManager) {
    }

    public void close() {
    }

    public AccessCheckResult check(AccessInfo accessInfo) {
      return new AccessCheckResult(true, "");
    }
  }

  public static class MockAccessCheckerAlwaysFalse implements AccessChecker {
    public MockAccessCheckerAlwaysFalse() {
    }

    public void close() {}

    public AccessCheckResult check(AccessInfo accessInfo) {
      return new AccessCheckResult(false, "MockAccessCheckerAlwaysFalse");
    }
  }
}
