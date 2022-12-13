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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * The class is to test the {@link LocalStorageManager}
 */
public class LocalStorageManagerTest {

  @BeforeAll
  public static void prepare() {
    ShuffleServerMetrics.register();
  }

  @AfterAll
  public static void clear() {
    ShuffleServerMetrics.clear();
  }

  private ShuffleDataFlushEvent toDataFlushEvent(String appId, int shuffleId, int startPartition) {
    return new ShuffleDataFlushEvent(
        1, // event id
        appId, // appId
        shuffleId, // shuffle id
        startPartition, // startPartition
        1, // endPartition
        1, // size
        new ArrayList<ShufflePartitionedBlock>(), // shuffleBlocks
        null, // valid
        null // shuffleBuffer
    );
  }

  @Test
  public void testStorageSelection() {
    String[] storagePaths = {"/tmp/rss-data1", "/tmp/rss-data2"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);

    String appId = "testCorruptedStorageApp";

    // case1: no corrupted storage, flush and read event of the same appId and shuffleId and startPartition
    // will always get the same storage
    ShuffleDataFlushEvent dataFlushEvent1 = toDataFlushEvent(appId, 1, 1);
    Storage storage1 = localStorageManager.selectStorage(dataFlushEvent1);

    ShuffleDataFlushEvent dataFlushEvent2 = toDataFlushEvent(appId, 1, 1);
    Storage storage2 = localStorageManager.selectStorage(dataFlushEvent2);

    ShuffleDataReadEvent dataReadEvent = new ShuffleDataReadEvent(appId, 1, 1);
    Storage storage3 = localStorageManager.selectStorage(dataReadEvent);
    assertEquals(storage1, storage2);
    assertEquals(storage1, storage3);

    // case2: one storage is corrupted, and it will switch to other storage at the first time of writing
    // event of (appId, shuffleId, startPartition)
    ((LocalStorage)storage1).markCorrupted();
    Storage storage4 = localStorageManager.selectStorage(dataFlushEvent1);
    assertNotEquals(storage4.getStoragePath(), storage1.getStoragePath());
    assertEquals(localStorageManager.selectStorage(dataReadEvent), storage4);

    // case3: one storage is corrupted when it happened after the original event has been written,
    // so it will switch to another storage for write and read event.
    LocalStorage mockedStorage = spy((LocalStorage)storage1);
    when(mockedStorage.containsWriteHandler(appId, 1, 1)).thenReturn(true);
    Storage storage5 = localStorageManager.selectStorage(dataFlushEvent1);
    Storage storage6 = localStorageManager.selectStorage(dataReadEvent);
    assertNotEquals(storage1, storage5);
    assertEquals(storage5, storage6);
  }

  @Test
  public void testInitLocalStorageManager() {
    String[] storagePaths = {"/tmp/rssdata", "/tmp/rssdata2"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);
    assertTrue(storages.size() == storagePaths.length);
    for (int i = 0; i < storagePaths.length; i++) {
      assertTrue(storagePaths[i].equals(storages.get(i).getBasePath()));
    }
  }

  @Test
  public void testInitializeLocalStorage() throws IOException {

    // case1: when no candidates, it should throw exception.
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/a/rss-data", "/b/rss-data"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 1);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    try {
      LocalStorageManager localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case2: when candidates exist, it should initialize successfully.
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/a/rss-data", "/tmp/rss-data-1"));
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);
    assertEquals(1, localStorageManager.getStorages().size());

    // case3: all ok
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/tmp/rss-data-1", "/tmp/rss-data-2"));
    localStorageManager = new LocalStorageManager(conf);
    assertEquals(2, localStorageManager.getStorages().size());

    // case4: only have 1 candidate, but exceed the number of rss.server.localstorage.initialize.max.fail.number
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/a/rss-data", "/tmp/rss-data-1"));
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 0L);
    try {
      localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case5: if failed=2, but lower than rss.server.localstorage.initialize.max.fail.number, should exit
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/a/rss-data", "/b/rss-data-1"));
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 10L);
    try {
      localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }
  }
}
