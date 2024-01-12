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

package org.apache.uniffle.shuffle.manager;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssShuffleManagerBaseTest {

  @Test
  public void testGetDefaultRemoteStorageInfo() {
    SparkConf sparkConf = new SparkConf();
    RemoteStorageInfo remoteStorageInfo =
        RssShuffleManagerBase.getDefaultRemoteStorageInfo(sparkConf);
    assertTrue(remoteStorageInfo.getConfItems().isEmpty());

    sparkConf.set("spark.rss.hadoop.fs.defaultFs", "hdfs://rbf-xxx/foo");
    remoteStorageInfo = RssShuffleManagerBase.getDefaultRemoteStorageInfo(sparkConf);
    assertEquals(remoteStorageInfo.getConfItems().size(), 1);
    assertEquals(remoteStorageInfo.getConfItems().get("fs.defaultFs"), "hdfs://rbf-xxx/foo");
  }
}
