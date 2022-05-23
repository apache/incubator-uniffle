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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

import com.tencent.rss.storage.common.LocalStorage;
import com.tencent.rss.storage.common.ShuffleFileInfo;
import com.tencent.rss.storage.common.StorageReadMetrics;
import com.tencent.rss.storage.factory.ShuffleUploadHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class ShuffleUploaderTest  {

  private static File base;

  @BeforeAll
  public static void setUp(@TempDir File tempDir) {
    base = tempDir;
    ShuffleServerMetrics.register();
  }

  @AfterAll
  public static void tearDown() {
    ShuffleServerMetrics.clear();
  }

  @Test
  public void selectShuffleFiles() {
    try {
      String app1 = "app-1";
      String shuffle1 = "1";
      String shuffleKey1 = String.join("/", app1, shuffle1);
      File partitionDir1 = makePartitionDir(app1, shuffle1, 1);
      File partitionDir2 = makePartitionDir(app1, shuffle1, 2);
      File partitionDir3 = makePartitionDir(app1, shuffle1, 3);
      File partitionDir4 = makePartitionDir(app1, shuffle1, 4);
      File partitionDir5 = makePartitionDir(app1, shuffle1, 5);

      File dataFile1 = new File(partitionDir1.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile2 = new File(partitionDir2.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile3 = new File(partitionDir3.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile4 = new File(partitionDir5.getAbsolutePath() + "/127.0.0.1-8080.data");

      File indexFile1 = new File(partitionDir1.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile2 = new File(partitionDir2.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile3 = new File(partitionDir3.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile5 = new File(partitionDir4.getAbsolutePath() + "/127.0.0.1-8080.index");

      List<File> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3, dataFile4);
      dataFiles.forEach(f -> writeFile(f, 10));

      List<File> indexFiles = Lists.newArrayList(indexFile1, indexFile2, indexFile3, indexFile5);
      indexFiles.forEach(f -> writeFile(f, 10));

      ShuffleServerConf conf = new ShuffleServerConf();
      conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 2);
      conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 3);
      conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 300);
      conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 1);
      conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
      conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://base");
      LocalStorage mockLocalStorage = mock(LocalStorage.class);
      when(mockLocalStorage.getBasePath()).thenReturn(base.getAbsolutePath());
      ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();

      when(mockLocalStorage.getSortedShuffleKeys(true, 4))
          .thenReturn(Lists.newArrayList(shuffleKey1, "zeroPartitionShuffleKey", "zeroSizeShuffleKey"));
      when(mockLocalStorage.getSortedShuffleKeys(true, 1))
          .thenReturn(Lists.newArrayList(shuffleKey1));
      when(mockLocalStorage.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(10L);
      when(mockLocalStorage.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf());
      when(mockLocalStorage.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(0L);
      when(mockLocalStorage.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf(1));
      when(mockLocalStorage.getNotUploadedSize(shuffleKey1))
          .thenReturn(30L);
      when(mockLocalStorage.getNotUploadedPartitions(shuffleKey1))
          .thenReturn(RoaringBitmap.bitmapOf(1, 2, 3));
      when(mockLocalStorage.getNotUploadedPartitions("zeroPartitionShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf());
      when(mockLocalStorage.getHighWaterMarkOfWrite()).thenReturn(100.0);
      when(mockLocalStorage.getLowWaterMarkOfWrite()).thenReturn(0.0);
      when(mockLocalStorage.getCapacity()).thenReturn(1024L);

      List<ShuffleFileInfo> shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertEquals(1, shuffleFileInfos.size());
      ShuffleFileInfo shuffleFileInfo = shuffleFileInfos.get(0);
      assertResult3(dataFiles, indexFiles, shuffleFileInfo);
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 5);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertEquals(3, shuffleFileInfos.size());
      assertResult1(dataFiles, indexFiles, shuffleFileInfos);
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(1, false);
      assertEquals(3, shuffleFileInfos.size());
      assertResult1(dataFiles, indexFiles, shuffleFileInfos);
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 20);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertEquals(1, shuffleFileInfos.size());
      shuffleFileInfo = shuffleFileInfos.get(0);
      assertResult3(dataFiles, indexFiles, shuffleFileInfo);
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 15);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertResult2(dataFiles, indexFiles, shuffleFileInfos);
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 12);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertResult2(dataFiles, indexFiles, shuffleFileInfos);

      when(mockLocalStorage.getSortedShuffleKeys(false, 4))
          .thenReturn(Lists.newArrayList(shuffleKey1, "zeroPartitionShuffleKey", "zeroSizeShuffleKey"));
      when(mockLocalStorage.getSortedShuffleKeys(false, 1))
          .thenReturn(Lists.newArrayList(shuffleKey1));
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 9);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, true);
      assertEquals(2, shuffleFileInfos.size());
      assertEquals(2, shuffleFileInfos.size());
      assertEquals(10, shuffleFileInfos.get(0).getSize());
      assertEquals(10, shuffleFileInfos.get(1).getSize());

      when(mockLocalStorage.getHighWaterMarkOfWrite()).thenReturn(95.0);
      when(mockLocalStorage.getLowWaterMarkOfWrite()).thenReturn(0.0);
      when(mockLocalStorage.getCapacity()).thenReturn(11L);
      conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
      shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, true);
      assertEquals(1, shuffleFileInfos.size());
      assertResult4(dataFiles, indexFiles, shuffleFileInfos);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectShuffleFilesRestrictionTest() {
    try {
      final int appNum = 4;
      final int partitionNum = 5;
      final List<String> shuffleKeys = Lists.newArrayList();
      for (int i = 0; i < appNum; ++i) {
        String appId = "app-" + i;
        String shuffleId = "1";
        String shuffleKey = String.join("/", appId, shuffleId);
        shuffleKeys.add(shuffleKey);

        for (int j = 0; j < partitionNum; ++j) {
          File partitionDir = makePartitionDir(appId, shuffleId, j);
          File dataFile = new File(partitionDir.getAbsolutePath() + "/127.0.0.1-8080.data");
          File indexFile = new File(partitionDir.getAbsolutePath() + "/127.0.0.1-8080.index");
          writeFile(dataFile, 5 * (i + 1));
          writeFile(indexFile, 5);
        }
      }

      LocalStorage mockLocalStorage = mock(LocalStorage.class);
      when(mockLocalStorage.getBasePath()).thenReturn(base.getAbsolutePath());
      when(mockLocalStorage.getSortedShuffleKeys(true, 4))
          .thenReturn(shuffleKeys);
      when(mockLocalStorage.getNotUploadedSize(any()))
          .thenReturn(partitionNum * 10L);
      when(mockLocalStorage.getNotUploadedPartitions(any()))
          .thenReturn(RoaringBitmap.bitmapOf(0, 1, 2, 3, 4));
      when(mockLocalStorage.getHighWaterMarkOfWrite()).thenReturn(100.0);
      when(mockLocalStorage.getLowWaterMarkOfWrite()).thenReturn(0.0);
      when(mockLocalStorage.getCapacity()).thenReturn(1024L);
      when(mockLocalStorage.getSortedShuffleKeys(false, 4))
          .thenReturn(shuffleKeys);
      ShuffleServerConf conf = new ShuffleServerConf();
      conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 4);
      conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 3);
      conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 10);
      conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 300);
      conf.setLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE, 10L);
      conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
      conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://base");

      ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
          .localStorage(mockLocalStorage)
          .serverId("127.0.0.1-8080")
          .configuration(conf)
          .build();

      List<ShuffleFileInfo> shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertEquals(15, shuffleFileInfos.size());
      // huge shuffle's segment num limited to thread num
      shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, true);
      assertEquals(4, shuffleFileInfos.size());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void assertResult1(List<File> dataFiles, List<File> indexFiles, List<ShuffleFileInfo> shuffleFileInfos) {
    for (int i = 0; i < 3; ++i) {
      assertEquals(dataFiles.get(i).getAbsolutePath(), shuffleFileInfos.get(i).getDataFiles().get(0).getAbsolutePath());
      assertEquals(indexFiles.get(i).getAbsolutePath(), shuffleFileInfos.get(i).getIndexFiles().get(0).getAbsolutePath());
      assertEquals(shuffleFileInfos.get(i).getSize(), 10L);
    }
  }

  private void assertResult2(List<File> dataFiles, List<File> indexFiles, List<ShuffleFileInfo> shuffleFileInfos) {
    assertEquals(2, shuffleFileInfos.size());
    assertEquals(20, shuffleFileInfos.get(0).getSize());
    assertEquals(10, shuffleFileInfos.get(1).getSize());
    assertEquals(dataFiles.get(0).getAbsolutePath(), shuffleFileInfos.get(0).getDataFiles().get(0).getAbsolutePath());
    assertEquals(indexFiles.get(0).getAbsolutePath(), shuffleFileInfos.get(0).getIndexFiles().get(0).getAbsolutePath());
    assertEquals(dataFiles.get(1).getAbsolutePath(), shuffleFileInfos.get(0).getDataFiles().get(1).getAbsolutePath());
    assertEquals(indexFiles.get(1).getAbsolutePath(), shuffleFileInfos.get(0).getIndexFiles().get(1).getAbsolutePath());
    assertEquals(dataFiles.get(2).getAbsolutePath(), shuffleFileInfos.get(1).getDataFiles().get(0).getAbsolutePath());
    assertEquals(indexFiles.get(2).getAbsolutePath(), shuffleFileInfos.get(1).getIndexFiles().get(0).getAbsolutePath());
  }

  private void assertResult3(List<File> dataFiles, List<File> indexFiles, ShuffleFileInfo shuffleFileInfo) {
    for (int i = 0; i < 3; ++i) {
      assertEquals(dataFiles.get(i).getAbsolutePath(), shuffleFileInfo.getDataFiles().get(i).getAbsolutePath());
      assertEquals(indexFiles.get(i).getAbsolutePath(), shuffleFileInfo.getIndexFiles().get(i).getAbsolutePath());
    }
  }

  private void assertResult4(List<File> dataFiles, List<File> indexFiles, List<ShuffleFileInfo> shuffleFileInfos) {
    assertEquals(1, shuffleFileInfos.size());
    assertEquals(10, shuffleFileInfos.get(0).getSize());
    assertEquals(dataFiles.get(0).getAbsolutePath(), shuffleFileInfos.get(0).getDataFiles().get(0).getAbsolutePath());
    assertEquals(indexFiles.get(0).getAbsolutePath(), shuffleFileInfos.get(0).getIndexFiles().get(0).getAbsolutePath());
  }

  @Test
  public void calculateUploadTimeTest() {
    LocalStorage mockLocalStorage = mock(LocalStorage.class);
    when(mockLocalStorage.getBasePath()).thenReturn(base.getAbsolutePath());
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
    conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 3);
    conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 300);
    conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 128);
    conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
    conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://base");
    ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
        .localStorage(mockLocalStorage)
        .serverId("prefix")
        .configuration(conf)
        .maxForceUploadExpireTimeS(13)
        .build();
    assertEquals(2, shuffleUploader.calculateUploadTime(0,0, false));
    assertEquals(2, shuffleUploader.calculateUploadTime(0, 128 * 1024, false));
    assertEquals(2, shuffleUploader.calculateUploadTime(0, 128 * 1024 * 1024, false));
    assertEquals(6, shuffleUploader.calculateUploadTime(0,3 * 128 * 1024 * 1024, false));
    assertEquals(12, shuffleUploader.calculateUploadTime(6 * 128 * 1024 * 1024,
        3 * 128 * 1024 * 1024, false));
    conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 2);
    shuffleUploader = new ShuffleUploader.Builder()
        .localStorage(mockLocalStorage)
        .serverId("prefix")
        .configuration(conf)
        .maxForceUploadExpireTimeS(10)
        .build();
    assertEquals(2, shuffleUploader.calculateUploadTime(0,0, false));
    assertEquals(2, shuffleUploader.calculateUploadTime(0,128 * 1024, false));
    assertEquals(2, shuffleUploader.calculateUploadTime(0,128 * 1024 * 1024, false));
    assertEquals(6, shuffleUploader.calculateUploadTime(0, 6 * 128 * 1024 * 1024, false));
    assertEquals(8, shuffleUploader.calculateUploadTime(4 * 128 * 1024 * 1024,
        6 * 128 * 1024 * 1024, false));

    shuffleUploader = new ShuffleUploader.Builder()
        .localStorage(mockLocalStorage)
        .configuration(conf)
        .maxForceUploadExpireTimeS(7)
        .build();
    assertEquals(7, shuffleUploader.calculateUploadTime(4 * 128 * 1024 * 1024,
        6 * 128 * 1024 * 1024, true));

    shuffleUploader = new ShuffleUploader.Builder()
        .localStorage(mockLocalStorage)
        .configuration(conf)
        .maxForceUploadExpireTimeS(1)
        .build();
    assertEquals(1, shuffleUploader.calculateUploadTime(0,0, true));
  }

  @Test
  public void uploadTest() {
    try {
      ShuffleUploader.Builder builder = new ShuffleUploader.Builder();
      ShuffleServerConf conf = new ShuffleServerConf();
      conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
      conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 1000);
      conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1);
      conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 2);
      conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
      conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://test");
      LocalStorage localStorage = LocalStorage.newBuilder()
          .capacity(100)
          .basePath(base.getAbsolutePath())
          .highWaterMarkOfWrite(50)
          .lowWaterMarkOfWrite(45)
          .shuffleExpiredTimeoutMs(1000)
          .build();
      builder
          .localStorage(localStorage)
          .configuration(conf)
          .serverId("test")
          .maxForceUploadExpireTimeS(1);
      ShuffleUploadHandlerFactory mockFactory = mock(ShuffleUploadHandlerFactory.class);
      ShuffleUploadHandler mockHandler = mock(ShuffleUploadHandler.class);
      when(mockFactory.createShuffleUploadHandler(any())).thenReturn(mockHandler);
      ShuffleUploadResult result0 = new ShuffleUploadResult(50, Lists.newArrayList(1, 2));
      ShuffleUploadResult result1 = new ShuffleUploadResult(90, Lists.newArrayList(1, 2, 3));
      ShuffleUploadResult result2 = new ShuffleUploadResult(10, Lists.newArrayList(1, 2));
      ShuffleUploadResult result3 = new ShuffleUploadResult(40, Lists.newArrayList(1, 3, 2, 4));
      when(mockHandler.upload(any(),any(), any())).thenReturn(result0).thenReturn(result1)
          .thenReturn(result2).thenReturn(result3);

      ShuffleUploader uploader = spy(builder.build());
      when(uploader.getHandlerFactory()).thenReturn(mockFactory);
      localStorage.createMetadataIfNotExist("key");
      localStorage.updateWrite("key", 70, Lists.newArrayList(1, 2, 3));
      File dir1 = new File(base.getAbsolutePath() + "/key/1-1/");
      dir1.mkdirs();
      File file1d = new File(base.getAbsolutePath() + "/key/1-1/test.data");
      file1d.createNewFile();
      File file1i = new File(base.getAbsolutePath() + "/key/1-1/test.index");
      file1i.createNewFile();
      File dir2 = new File(base.getAbsolutePath() + "/key/2-2/");
      dir2.mkdirs();
      File file2d = new File(base.getAbsolutePath() + "/key/2-2/test.data");
      file2d.createNewFile();
      File file2i = new File(base.getAbsolutePath() + "/key/2-2/test.index");
      file2i.createNewFile();
      File dir3 = new File(base.getAbsolutePath() + "/key/3-3/");
      dir3.mkdirs();
      File file3d = new File(base.getAbsolutePath() + "/key/3-3/test.data");
      file3d.createNewFile();
      File file3i = new File(base.getAbsolutePath() + "/key/3-3/test.index");
      file3i.createNewFile();
      byte[] data = new byte[20];
      new Random().nextBytes(data);
      try (OutputStream out = new FileOutputStream(file1d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[30];
      try (OutputStream out = new FileOutputStream(file2d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[20];
      try (OutputStream out = new FileOutputStream(file3d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      uploader.upload();
      assertEquals(20, localStorage.getNotUploadedSize("key"));
      assertEquals(1, localStorage.getNotUploadedPartitions("key").getCardinality());
      assertTrue(localStorage.getNotUploadedPartitions("key").contains(3));
      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      localStorage.updateWrite("key", 70, Lists.newArrayList(1, 2));
      file1d.createNewFile();
      file1i.createNewFile();
      file2d.createNewFile();
      file2i.createNewFile();
      data = new byte[30];
      new Random().nextBytes(data);
      try (OutputStream out = new FileOutputStream(file1d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[40];
      try (OutputStream out = new FileOutputStream(file2d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      uploader.upload();
      assertEquals(0, localStorage.getNotUploadedSize("key"));
      assertTrue(localStorage.getNotUploadedPartitions("key").isEmpty());
      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertFalse(file3d.exists());
      assertFalse(file3i.exists());

      localStorage.updateWrite("key", 30, Lists.newArrayList(1, 2, 3));
      file1d.createNewFile();
      file1i.createNewFile();
      file2d.createNewFile();
      file2i.createNewFile();
      file3i.createNewFile();
      file3d.createNewFile();
      data = new byte[5];
      new Random().nextBytes(data);
      try (OutputStream out = new FileOutputStream(file1d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[5];
      try (OutputStream out = new FileOutputStream(file2d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[20];
      try (OutputStream out = new FileOutputStream(file3d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      uploader.upload();
      assertEquals(30, localStorage.getNotUploadedSize("key"));
      assertEquals(3, localStorage.getNotUploadedPartitions("key").getCardinality());

      localStorage.prepareStartRead("key");
      uploader.upload();
      assertEquals(20, localStorage.getNotUploadedSize("key"));
      assertEquals(1, localStorage.getNotUploadedPartitions("key").getCardinality());
      assertTrue(file1d.exists());
      assertTrue(file1i.exists());
      assertTrue(file2d.exists());
      assertTrue(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      localStorage.updateShuffleLastReadTs("key");
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
      assertTrue(file1d.exists());
      assertTrue(file1i.exists());
      assertTrue(file2d.exists());
      assertTrue(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      localStorage.updateShuffleLastReadTs("key");
      localStorage.updateWrite("key", 20, Lists.newArrayList(1, 2, 4));
      data = new byte[10];
      new Random().nextBytes(data);
      try (OutputStream out = new FileOutputStream(file1d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      data = new byte[10];
      try (OutputStream out = new FileOutputStream(file2d)) {
        out.write(data);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      uploader.upload();
      assertEquals(0, localStorage.getNotUploadedSize("key"));
      assertTrue(localStorage.getNotUploadedPartitions("key").isEmpty());
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertFalse(file3d.exists());
      assertFalse(file3i.exists());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void cleanTest(@TempDir File tempDir) {
    try {

      ShuffleServerConf conf = new ShuffleServerConf();
      conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
      conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 1000);
      conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1);
      conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 2);
      conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
      conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://test");

      LocalStorage localStorage = LocalStorage.newBuilder()
          .basePath(tempDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(100)
          .lowWaterMarkOfWrite(100)
          .capacity(100)
          .cleanIntervalMs(5000)
          .shuffleExpiredTimeoutMs(1)
          .build();

      File baseDir = new File(tempDir, "app-1");
      baseDir.mkdir();
      assertTrue(baseDir.exists());
      File dir1 = new File(baseDir, "1");
      File dir2 = new File(baseDir, "2");
      dir1.mkdir();
      dir2.mkdir();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      localStorage.createMetadataIfNotExist("app-1/1");
      localStorage.createMetadataIfNotExist("app-1/2");
      localStorage.updateWrite("app-1/1", 0, Lists.newArrayList());
      localStorage.updateWrite("app-1/2", 0, Lists.newArrayList());

      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      localStorage.updateWrite("app-1/1", 25, Lists.newArrayList(1));
      localStorage.updateWrite("app-1/2", 35, Lists.newArrayList(2));
      assertEquals(60, localStorage.getDiskSize());

      assertTrue(dir1.exists());
      assertTrue(dir2.exists());

      ShuffleUploader uploader = new ShuffleUploader.Builder()
          .localStorage(localStorage)
          .configuration(conf)
          .serverId("test")
          .maxForceUploadExpireTimeS(1)
          .build();
      uploader.cleanUploadedShuffle(Sets.newHashSet("app-1/1"));

      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      localStorage.updateReadMetrics(new StorageReadMetrics("app-1", 1));
      uploader.cleanUploadedShuffle(Sets.newHashSet("app-1/1"));
      assertTrue(dir1.exists());
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
      uploader.cleanUploadedShuffle(Sets.newHashSet("app-1/1"));

      assertFalse(dir1.exists());
      assertTrue(dir2.exists());
      localStorage.updateReadMetrics(new StorageReadMetrics("app-1/2", 2));
      uploader.cleanUploadedShuffle(Sets.newHashSet("app-1/2"));
      assertTrue(dir2.exists());
      assertEquals(35, localStorage.getDiskSize());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void delayCleanTest(@TempDir File tempDir) {
    LocalStorage storage = LocalStorage.newBuilder()
        .basePath(tempDir.getAbsolutePath())
        .cleanupThreshold(0)
        .highWaterMarkOfWrite(100)
        .lowWaterMarkOfWrite(100)
        .capacity(100)
        .cleanIntervalMs(1000)
        .shuffleExpiredTimeoutMs(1)
        .build();

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setInteger(ShuffleServerConf.UPLOADER_THREAD_NUM, 1);
    conf.setLong(ShuffleServerConf.UPLOADER_INTERVAL_MS, 1000);
    conf.setLong(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB, 1);
    conf.setLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS, 2);
    conf.setString(ShuffleServerConf.UPLOAD_STORAGE_TYPE, StorageType.HDFS.name());
    conf.setString(ShuffleServerConf.UPLOADER_BASE_PATH, "hdfs://test");

    storage.createMetadataIfNotExist("key1");
    storage.createMetadataIfNotExist("key2");
    storage.createMetadataIfNotExist("key3");
    storage.updateWrite("key1", 100, Lists.newArrayList());
    storage.updateWrite("key2", 50, Lists.newArrayList());
    storage.updateWrite("key3", 95, Lists.newArrayList());
    assertEquals(3, storage.getShuffleMetaSet().size());
    assertTrue(storage.getShuffleMetaSet().contains("key1"));
    assertTrue(storage.getShuffleMetaSet().contains("key2"));
    assertTrue(storage.getShuffleMetaSet().contains("key3"));
    assertEquals(245, storage.getDiskSize());

    ShuffleUploader uploader = new ShuffleUploader.Builder()
        .localStorage(storage)
        .configuration(conf)
        .serverId("test")
        .maxForceUploadExpireTimeS(1)
        .build();

    storage.updateShuffleLastReadTs("key1");
    storage.updateShuffleLastReadTs("key2");
    storage.updateShuffleLastReadTs("key3");
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    storage.getExpiredShuffleKeys().offer("key1");
    storage.getExpiredShuffleKeys().offer("key2");
    assertEquals(2, storage.getExpiredShuffleKeys().size());
    uploader.cleanUploadedShuffle(Sets.newHashSet());
    assertEquals(1, storage.getShuffleMetaSet().size());
    assertEquals(95, storage.getDiskSize());
    assertTrue(storage.getShuffleMetaSet().contains("key3"));
    assertEquals(0, storage.getExpiredShuffleKeys().size());

    storage.getExpiredShuffleKeys().offer("key3");
    assertEquals(1, storage.getExpiredShuffleKeys().size());
    uploader.cleanUploadedShuffle(Sets.newHashSet());
    assertEquals(0, storage.getShuffleMetaSet().size());
    assertEquals(0, storage.getDiskSize());
    assertEquals(0, storage.getExpiredShuffleKeys().size());
  }

  private File makePartitionDir(String appId, String shuffleId, int partitionId) {
    File dir = base.toPath().resolve(appId).resolve(shuffleId)
        .resolve(partitionId + "-" + partitionId).toFile();
    dir.mkdirs();
    return dir;
  }

  private void writeFile(File f, int size) {
    byte[] data1 = new byte[size];
    new Random().nextBytes(data1);
    try (OutputStream out = new FileOutputStream(f)) {
      out.write(data1);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
