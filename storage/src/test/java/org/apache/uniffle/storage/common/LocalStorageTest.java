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

package org.apache.uniffle.storage.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.storage.StorageMedia;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalStorageTest {

  private static File testBaseDir;
  private static File testBaseDirWithoutPermission;
  private static String mountPoint;

  @BeforeAll
  public static void setUp(@TempDir File tempDir) throws IOException {
    testBaseDir = new File(tempDir, "test");
    testBaseDirWithoutPermission = new File(tempDir, "test-no-permission");
    testBaseDir.mkdir();
    testBaseDirWithoutPermission.mkdirs();
    try {
      mountPoint = Files.getFileStore(testBaseDir.toPath()).name();
    } catch (IOException ioe) {
      // pass
    }
  }

  @AfterAll
  public static void tearDown() {
    testBaseDir.delete();
  }

  private LocalStorage createTestStorage(File baseDir) {
    return LocalStorage.newBuilder()
        .basePath(baseDir.getAbsolutePath())
        .highWaterMarkOfWrite(95)
        .lowWaterMarkOfWrite(80)
        .capacity(100)
        .build();
  }

  @Test
  public void canWriteTest() {
    LocalStorage item = createTestStorage(testBaseDir);

    item.updateServiceUsedBytes(20);
    assertTrue(item.canWrite());
    item.updateServiceUsedBytes(item.getServiceUsedBytes() + 65);
    assertTrue(item.canWrite());
    item.updateServiceUsedBytes(item.getServiceUsedBytes() + 10);
    assertFalse(item.canWrite());
    item.updateServiceUsedBytes(item.getServiceUsedBytes() - 10);
    assertFalse(item.canWrite());
    item.updateServiceUsedBytes(item.getServiceUsedBytes() - 10);
    assertTrue(item.canWrite());
  }

  @Test
  public void getCapacityInitTest() {
    LocalStorage item =
        LocalStorage.newBuilder()
            .basePath(testBaseDir.getAbsolutePath())
            .highWaterMarkOfWrite(95)
            .lowWaterMarkOfWrite(80)
            .capacity(-1)
            .ratio(0.1)
            .build();
    assertEquals((long) (testBaseDir.getTotalSpace() * 0.1), item.getCapacity());
  }

  @Test
  public void baseDirectoryInitTest() throws IOException {
    // empty and writable base dir
    File newBaseDir = new File(testBaseDirWithoutPermission, "test-new");
    assertTrue(newBaseDir.mkdirs());
    // then remove write permission to parent dir.
    Set<PosixFilePermission> perms = Sets.newHashSet();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.OWNER_EXECUTE);
    Files.setPosixFilePermissions(testBaseDirWithoutPermission.toPath(), perms);
    createTestStorage(newBaseDir);
    // non-empty and writable base dir
    File childDir = new File(newBaseDir, "placeholder");
    assertTrue(childDir.mkdirs());
    createTestStorage(newBaseDir);
    // base dir is configured to a wrong file instead of dir
    File emptyFile = new File(newBaseDir, "empty_file");
    assertTrue(emptyFile.createNewFile());
    assertThrows(
        RuntimeException.class,
        () -> {
          createTestStorage(emptyFile);
        });

    // not existed base dir should work
    File notExisted = new File(newBaseDir, "not_existed");
    createTestStorage(notExisted);
  }

  @Test
  public void diskStorageInfoTest() {
    LocalStorage item =
        LocalStorage.newBuilder()
            .basePath(testBaseDir.getAbsolutePath())
            .highWaterMarkOfWrite(95)
            .lowWaterMarkOfWrite(80)
            .capacity(100)
            .build();
    assertEquals(mountPoint, item.getMountPoint());
    assertNull(item.getStorageMedia());

    LocalStorage itemWithStorageType =
        LocalStorage.newBuilder()
            .basePath(testBaseDir.getAbsolutePath())
            .highWaterMarkOfWrite(95)
            .lowWaterMarkOfWrite(80)
            .capacity(100)
            .localStorageMedia(StorageMedia.SSD)
            .build();
    assertEquals(StorageMedia.SSD, itemWithStorageType.getStorageMedia());
  }

  @Test
  public void writeHandlerTest() {
    LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath()).build();
    String appId = "writeHandlerTest";
    assertFalse(item.containsWriteHandler(appId, 0, 1));
    String[] storageBasePaths = {testBaseDir.getAbsolutePath()};
    CreateShuffleWriteHandlerRequest request =
        new CreateShuffleWriteHandlerRequest(
            StorageType.LOCALFILE.name(), appId, 0, 1, 1, storageBasePaths, "ss1", null, 1, null);
    item.getOrCreateWriteHandler(request);
    assertTrue(item.containsWriteHandler(appId, 0, 1));
  }

  @Test
  public void canWriteTestWithDiskCapacityCheck() {
    // capacity < diskCapacity
    LocalStorage localStorage =
        LocalStorage.newBuilder()
            .basePath(testBaseDir.getAbsolutePath())
            .highWaterMarkOfWrite(95)
            .lowWaterMarkOfWrite(80)
            .capacity(100)
            .enableDiskCapacityWatermarkCheck()
            .build();

    localStorage.updateServiceUsedBytes(localStorage.getServiceUsedBytes() + 20);
    assertTrue(localStorage.canWrite());
    localStorage.updateServiceUsedBytes(localStorage.getServiceUsedBytes() + 65);
    assertTrue(localStorage.canWrite());

    final long diskCapacity = localStorage.getDiskCapacity();
    localStorage.updateDiskAvailableBytes((long) (diskCapacity * (1 - 0.96)));
    assertFalse(localStorage.canWrite());

    localStorage.updateDiskAvailableBytes((long) (diskCapacity * (1 - 0.60)));
    assertFalse(localStorage.canWrite());
    localStorage.updateServiceUsedBytes(localStorage.getServiceUsedBytes() - 10);
    assertTrue(localStorage.canWrite());

    // capacity = diskCapacity
    localStorage =
        LocalStorage.newBuilder()
            .basePath(testBaseDir.getAbsolutePath())
            .highWaterMarkOfWrite(95)
            .lowWaterMarkOfWrite(80)
            .capacity(-1)
            .ratio(1)
            .enableDiskCapacityWatermarkCheck()
            .build();

    localStorage.updateDiskAvailableBytes((long) (diskCapacity * (1 - 0.96)));
    assertFalse(localStorage.canWrite());

    localStorage.updateDiskAvailableBytes((long) (diskCapacity * (1 - 0.60)));
    assertTrue(localStorage.canWrite());
  }
}
