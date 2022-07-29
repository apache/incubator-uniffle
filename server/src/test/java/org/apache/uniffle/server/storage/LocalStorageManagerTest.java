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

package org.apache.uniffle.server.storage;

import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.StorageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LocalStorageManagerTest {

  private static LocalStorageManager localStorageManager;
  private static String[] storagePaths = {"/tmp/rssdata", "/tmp/rssdata2"};

  @BeforeAll
  public static void prepare() {
    ShuffleServerMetrics.register();
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, String.join(",", storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    localStorageManager = new LocalStorageManager(conf);
  }

  @AfterAll
  public static void clear() {
    ShuffleServerMetrics.clear();
  }

  @Test
  public void testInitLocalStorageManager() {
    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);
    assertTrue(storages.size() == storagePaths.length);
    for (int i = 0; i < storagePaths.length; i++) {
      assertTrue(storagePaths[i].equals(storages.get(i).getBasePath()));
    }
  }
}

