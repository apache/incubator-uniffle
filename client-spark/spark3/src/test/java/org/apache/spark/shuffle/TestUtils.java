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

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.writer.DataPusher;

import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.util.BlockId;

public class TestUtils {

  private TestUtils() {}

  public static RssShuffleManager createShuffleManager(
      SparkConf conf,
      Boolean isDriver,
      DataPusher dataPusher,
      Map<String, Set<BlockId>> successBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker) {
    return new RssShuffleManager(
        conf, isDriver, dataPusher, successBlockIds, taskToFailedBlockSendTracker);
  }

  public static boolean isMacOnAppleSilicon() {
    return SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64");
  }

  public static ShuffleBlockInfo createMockBlockOnlyBlockId(BlockId blockId) {
    return new ShuffleBlockInfo(1, 1, blockId, 1, 1, new byte[1], null, 1, 100, 1);
  }
}
