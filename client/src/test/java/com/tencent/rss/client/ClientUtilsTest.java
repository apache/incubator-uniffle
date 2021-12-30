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

package com.tencent.rss.client;

import org.junit.Test;

import com.tencent.rss.client.util.ClientUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientUtilsTest {

  private static String EXCEPTION_EXPECTED = "Exception excepted";

  @Test
  public void getBlockIdTest() {
    // max value of blockId
    assertEquals(
        new Long(9223372036854775807L), ClientUtils.getBlockId(16777215, 1048575, 524287));
    // just a random test
    assertEquals(
        new Long(1759218709299300L), ClientUtils.getBlockId(100, 100, 100));
    // min value of blockId
    assertEquals(
        new Long(0L), ClientUtils.getBlockId(0, 0, 0));
    try {
      ClientUtils.getBlockId(16777216, 0, 0);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support partitionId[16777216], the max value should be 16777215"));
    }
    try {
      ClientUtils.getBlockId(0, 1048576, 0);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support taskAttemptId[1048576], the max value should be 1048575"));
    }
    try {
      ClientUtils.getBlockId(0, 0, 524288);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support sequence[524288], the max value should be 524287"));
    }
  }
}
