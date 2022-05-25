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

import org.junit.jupiter.api.Test;

import com.tencent.rss.client.util.ClientUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ClientUtilsTest {

  private static String EXCEPTION_EXPECTED = "Exception excepted";

  @Test
  public void getBlockIdTest() {
    // max value of blockId
    assertEquals(
        new Long(854558029292503039L), ClientUtils.getBlockId(16777215, 1048575, 24287));
    // just a random test
    assertEquals(
        new Long(3518437418598500L), ClientUtils.getBlockId(100, 100, 100));
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
      ClientUtils.getBlockId(0, 2097152, 0);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support taskAttemptId[2097152], the max value should be 2097151"));
    }
    try {
      ClientUtils.getBlockId(0, 0, 262144);
      fail(EXCEPTION_EXPECTED);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't support sequence[262144], the max value should be 262143"));
    }
  }
}
