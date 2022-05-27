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

package com.tencent.rss.common;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteStorageInfoTest {
  @Test
  public void test() {
    final String testPath = "hdfs://test";
    final String confString = "k1=v1,k2=v2";
    final Map<String, String> confMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    assertTrue(new RemoteStorageInfo("", "test").isEmpty());
    RemoteStorageInfo remoteStorageInfo = new RemoteStorageInfo(testPath);
    assertEquals(testPath, remoteStorageInfo.getPath());
    assertTrue(remoteStorageInfo.getConfItems().isEmpty());
    assertEquals("", remoteStorageInfo.getConfString());

    remoteStorageInfo =
        new RemoteStorageInfo(testPath, confMap);
    assertEquals(2, remoteStorageInfo.getConfItems().size());
    assertEquals(testPath, remoteStorageInfo.getPath());
    assertEquals(confString, remoteStorageInfo.getConfString());
    assertEquals("v1", remoteStorageInfo.getConfItems().get("k1"));
    assertEquals("v2", remoteStorageInfo.getConfItems().get("k2"));

    RemoteStorageInfo remoteStorageInfo1 = new RemoteStorageInfo(testPath, confString);
    assertEquals(remoteStorageInfo1, remoteStorageInfo);
    RemoteStorageInfo remoteStorageInfo2 =
        new RemoteStorageInfo(testPath, ImmutableMap.of("k1", "v11"));
    assertNotEquals(remoteStorageInfo1, remoteStorageInfo2);
    RemoteStorageInfo remoteStorageInfo3 =
        new RemoteStorageInfo(testPath + "3", confMap);
    assertNotEquals(remoteStorageInfo1, remoteStorageInfo3);
  }
}
