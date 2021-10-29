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

package com.tencent.rss.storage.common;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShuffleFileInfoTest {

  @Test
  public void test() {
    try {
      ShuffleFileInfo shuffleFileInfo = new ShuffleFileInfo();
      shuffleFileInfo.getDataFiles().add(File.createTempFile("dummy-data-file", ".data"));
      shuffleFileInfo.setKey("key");
      assertFalse(shuffleFileInfo.isValid());

      shuffleFileInfo.getIndexFiles().add(File.createTempFile("dummy-data-file", ".index"));
      shuffleFileInfo.getPartitions().add(12);
      shuffleFileInfo.setSize(1024 * 1024 * 32);
      assertTrue(shuffleFileInfo.isValid());
      assertFalse(shuffleFileInfo.shouldCombine(32));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
