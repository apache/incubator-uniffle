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

package com.tencent.rss.server;

import com.tencent.rss.common.config.RssBaseConf;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiStorageManagerTest {

  @Test
  public void ConstructorTest() throws IOException {
    ShuffleServerConf conf = new ShuffleServerConf();
    boolean isException = false;
    try {
        new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("Base path dirs must not be empty"));
    }
    assertTrue(isException);
    isException = false;
    TemporaryFolder tmp = new TemporaryFolder();
    tmp.create();
    conf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, tmp.getRoot().getAbsolutePath());
    conf.set(ShuffleServerConf.DISK_CAPACITY, -1L);
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("Capacity must be larger than zero"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.DISK_CAPACITY, 100L);
    conf.set(ShuffleServerConf.CLEANUP_THRESHOLD, -1.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("cleanupThreshold must be between 0 and 100"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.CLEANUP_THRESHOLD, 999.9);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("cleanupThreshold must be between 0 and 100"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.CLEANUP_THRESHOLD, 85.1);
    conf.set(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 10.0);
    conf.set(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE, 20.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 120.0);
    conf.set(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE, -10.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("lowWaterMarkOfWrite must be larger than zero"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 120.0);
    conf.set(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE,  10.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("highWaterMarkOfWrite must be smaller than 100"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 90.0);
    conf.set(ShuffleServerConf.UPLOADER_THREAD_NUM, -1);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadThreadNum must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
    conf.set(ShuffleServerConf.UPLOADER_INTERVAL_MS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadIntervalMs must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.UPLOADER_INTERVAL_MS, 1L);
    conf.set(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadCombineThresholdMB must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    conf.set(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("referenceUploadSpeedMbps must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("hdfsBasePath couldn't be empty"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.HDFS_BASE_PATH, "testPath");
    conf.set(ShuffleServerConf.UPLOAD_STORAGE_TYPE, "LOCALFILE");
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("ploadRemoteStorageType couldn't be LOCALFILE or FILE"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.UPLOAD_STORAGE_TYPE, "XXXX");
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("No enum constant"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.UPLOAD_STORAGE_TYPE, "HDFS");

    conf.set(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L * 1024 * 1024L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("Disk Available Capacity"));
      isException = true;
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.DISK_CAPACITY, 100L);

    conf.set(ShuffleServerConf.CLEANUP_INTERVAL_MS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("cleanupInterval must be larger than zero"));
      isException = true;
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.CLEANUP_INTERVAL_MS, 100L);

    conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, - 1);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("The value of maxShuffleSize must be positive"));
      isException = true;
    }
    assertTrue(isException);
    conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 100);
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      fail();
    }
  }
}