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

package org.apache.spark.shuffle;

import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.util.EventLoop;

public class TestUtils {

  private TestUtils() {
  }

  public static RssShuffleManager createShuffleManager(
      SparkConf conf,
      Boolean isDriver,
      EventLoop loop,
      Map<String, Set<Long>> successBlockIds,
      Map<String, Set<Long>> failBlockIds) {
    return new RssShuffleManager(conf, isDriver, loop, successBlockIds, failBlockIds);
  }
}
