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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LocalStorageCheckerTest {

  @Test
  public void testGetUniffleUsedSpace(@TempDir File tempDir) throws IOException {
    File file1 = createTempFile(tempDir, "file1.txt", 1000);
    File file2 = createTempFile(tempDir, "file2.txt", 2000);
    File subdir1 = createTempSubDirectory(tempDir, "subdir1");
    File file3 = createTempFile(subdir1, "file3.txt", 500);
    File subdir2 = createTempSubDirectory(subdir1, "subdir2");
    File file4 = createTempFile(subdir2, "file4.txt", 1500);

    // Call the method to calculate disk usage
    long calculatedUsage = LocalStorageChecker.getUniffleUsedSpace(tempDir);

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
