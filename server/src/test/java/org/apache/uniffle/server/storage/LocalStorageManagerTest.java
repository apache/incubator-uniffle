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

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.StorageType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The class is to test the {@link LocalStorageManager}
 */
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

  @Test
  public void testInitializeLocalStorage() throws IOException {
    String tmpDir1 = Files.createTempDirectory("rss-data1").toString();
    String tmpDir2 = Files.createTempDirectory("rss-data2").toString();

    // case1: when no candidates, it should throw exception.
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, tmpDir1 + "," + tmpDir2);
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setInteger(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 1);
    MockedLocalStorageBuilder.setErrorPath(Arrays.asList(tmpDir1, tmpDir2));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    try {
      LocalStorageManager localStorageManager = new LocalStorageManager(conf, new MockedLocalStorageBuilder());
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case2: when candidates exist, it should initialize successfully.
    MockedLocalStorageBuilder.setErrorPath(Arrays.asList(tmpDir1));
    LocalStorageManager localStorageManager = new LocalStorageManager(conf, new MockedLocalStorageBuilder());
    assertEquals(1, localStorageManager.getStorages().size());

    // case3: all ok
    MockedLocalStorageBuilder.setErrorPath(Collections.EMPTY_LIST);
    localStorageManager = new LocalStorageManager(conf, new MockedLocalStorageBuilder());
    assertEquals(2, localStorageManager.getStorages().size());
  }

  public static class MockedLocalStorageBuilder extends LocalStorage.Builder {
    private static List<String> errorPaths;

    public static void setErrorPath(List<String> errorPaths) {
      MockedLocalStorageBuilder.errorPaths = errorPaths;
    }

    public LocalStorage build() {
      if (errorPaths.contains(getBasePath())) {
        throw new RuntimeException("ERROR Disk.");
      }
      return super.build();
    }
  }
}