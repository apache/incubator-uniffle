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

package com.tencent.rss.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.tencent.rss.common.util.ExitUtils.ExitException;

import org.junit.jupiter.api.Test;

public class ExitUtilsTest {

  @Test
  public void test() {
    try {
    final int status = -1;
    final String testExitMessage = "testExitMessage";
    try {
      ExitUtils.disableSystemExit();
      ExitUtils.terminate(status, testExitMessage, null, null);
      fail();
    } catch (ExitException e) {
      assertEquals(status, e.getStatus());
      assertEquals(testExitMessage, e.getMessage());
    }

    final Thread t = new Thread(null, () -> {
      throw new AssertionError("TestUncaughtException");
    }, "testThread");
    t.start();
    t.join();
  } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }
}
