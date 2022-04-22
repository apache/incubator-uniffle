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

package org.apache.hadoop.mapreduce;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.junit.BeforeClass;
import org.junit.Test;

public class LargeSorterTest extends MRIntegrationTestBase {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Test
  public void largeSorterTest() throws Exception {
    run();
  }

  @Override
  protected void updateRssConfiguration(Configuration jobConf) {
    jobConf.setInt(LargeSorter.NUM_MAP_TASKS, 1);
    jobConf.setInt(LargeSorter.MBS_PER_MAP, 128);
  }

  @Override
  protected Tool getTestTool() {
    return new LargeSorter();
  }
}
