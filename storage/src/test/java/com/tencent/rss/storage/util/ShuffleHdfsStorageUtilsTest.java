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

package com.tencent.rss.storage.util;

import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsFileWriter;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleHdfsStorageUtilsTest extends HdfsTestBase {

  @Test
  public void testUploadFile(@TempDir File tempDir) {
    FileOutputStream fileOut = null;
    DataOutputStream dataOut = null;
    try {
      File file = new File(tempDir, "test");
      fileOut = new FileOutputStream(file);
      dataOut = new DataOutputStream(fileOut);
      byte[] buf = new byte[2096];
      new Random().nextBytes(buf);
      dataOut.write(buf);
      dataOut.close();
      fileOut.close();
      String path = HDFS_URI + "test";
      HdfsFileWriter writer = new HdfsFileWriter(new Path(path), conf);
      long size = ShuffleStorageUtils.uploadFile(file, writer, 1024);
      assertEquals(2096, size);
      size = ShuffleStorageUtils.uploadFile(file, writer, 100);
      assertEquals(2096, size);
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
