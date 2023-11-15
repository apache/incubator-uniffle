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

package org.apache.uniffle.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.storage.common.LocalStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class LocalStorageCheckerTest {

  @BeforeEach
  public void beforeEach() {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
  }

  @Test
  public void testWatermarkLimit(@TempDir File tempDir) throws Exception {
    LocalStorage storage = LocalStorage.newBuilder()
        .basePath(tempDir.getAbsolutePath())
        .build();

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setDouble(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE, 80.0);
    conf.setDouble(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE, 60.0);
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(), tempDir.getAbsolutePath());
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());

    LocalStorageChecker checker = new LocalStorageChecker(
        conf,
        Arrays.asList(storage)
    );

    checker = spy(checker);
    checker.resetStorages(storage);

    doReturn(1000L).when(checker).getTotalSpace(any());
    doReturn(600L).when(checker).getWholeDiskUsedSpace(any());

    // case1
    checker.checkIsHealthy();
    assertFalse(storage.isWatermarkLimitTriggered());
    assertTrue(storage.canWrite());

    // case2
    doReturn(850L).when(checker).getWholeDiskUsedSpace(any());
    checker.checkIsHealthy();
    assertTrue(storage.isWatermarkLimitTriggered());
    assertFalse(storage.canWrite());

    // case3. re-check
    checker.checkIsHealthy();
    assertTrue(storage.isWatermarkLimitTriggered());
    assertFalse(storage.canWrite());

    // case4.
    doReturn(645L).when(checker).getWholeDiskUsedSpace(any());
    checker.checkIsHealthy();
    assertTrue(storage.isWatermarkLimitTriggered());
    assertFalse(storage.canWrite());

    // case5. recover
    doReturn(545L).when(checker).getWholeDiskUsedSpace(any());
    checker.checkIsHealthy();
    assertFalse(storage.isWatermarkLimitTriggered());
    assertTrue(storage.canWrite());
  }

  @Test
  public void testGetUniffleUsedSpace(@TempDir File tempDir) throws IOException {
    File file1 = createTempFile(tempDir, "file1.txt", 1000);
    File file2 = createTempFile(tempDir, "file2.txt", 2000);
    File subdir1 = createTempSubDirectory(tempDir, "subdir1");
    File file3 = createTempFile(subdir1, "file3.txt", 500);
    File subdir2 = createTempSubDirectory(subdir1, "subdir2");
    File file4 = createTempFile(subdir2, "file4.txt", 1500);

    // Call the method to calculate disk usage
    long calculatedUsage = LocalStorageChecker.getServiceUsedSpace(tempDir);

    // The expected total usage should be the sum of file1 + file2 + file3 + file4
    long expectedUsage = file1.length() + file2.length() + file3.length() + file4.length();

    // Assert that the calculated result matches the expected value
    Assertions.assertEquals(expectedUsage, calculatedUsage);
  }

  private File createTempFile(File directory, String fileName, long fileSize) throws IOException {
    File file = new File(directory, fileName);
    Files.write(file.toPath(), new byte[(int) fileSize]);
    return file;
  }

  private File createTempSubDirectory(File parentDirectory, String directoryName) {
    File subDir = new File(parentDirectory, directoryName);
    subDir.mkdirs();
    return subDir;
  }
}
