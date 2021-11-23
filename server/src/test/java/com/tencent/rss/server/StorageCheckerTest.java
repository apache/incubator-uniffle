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

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageCheckerTest {

  private int callTimes = 0;

  @Test
  public void checkTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "st1,st2,st3");
    conf.set(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE, 55.0);
    LocalStorageChecker checker = new MockStorageChecker(conf);

    assertTrue(checker.checkIsHealthy());

    callTimes++;
    assertTrue(checker.checkIsHealthy());

    callTimes++;
    assertFalse(checker.checkIsHealthy());

    callTimes++;
    assertTrue(checker.checkIsHealthy());
    conf.set(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE, 80.0);
    checker = new MockStorageChecker(conf);
    assertFalse(checker.checkIsHealthy());

    callTimes++;
    checker.checkIsHealthy();
    assertTrue(checker.checkIsHealthy());
  }

  private class MockStorageChecker extends LocalStorageChecker {
    public MockStorageChecker(ShuffleServerConf conf) {
      super(conf);
    }

    @Override
    long getTotalSpace(File file) {
      return 1000;
    }

    // we mock this method, and will return different values according
    // to call times.
    @Override
    long getUsedSpace(File file) {
      long result = 0;
      switch (file.getPath()) {
        case "st1":
          switch (callTimes) {
            case 0:
              result = 100;
              break;
            case 1:
            case 2:
            case 3:
              result = 900;
              break;
            case 4:
              result = 150;
              break;
            default:
              break;
          }
          break;
        case "st2":
          switch (callTimes) {
            case 0:
            case 1:
              result = 200;
              break;
            case 2:
              result = 900;
              break;
            case 3:
              result = 400;
              break;
            case 4:
              result = 100;
            default:
              break;
          }
          break;
        case "st3":
          switch (callTimes) {
            case 0:
            case 1:
            case 2:
            case 3:
              result = 300;
              break;
            default:
              break;
          }
          break;
      }
      return result;
    }
  }
}
