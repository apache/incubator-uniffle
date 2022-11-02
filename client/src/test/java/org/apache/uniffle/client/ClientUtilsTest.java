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

package org.apache.uniffle.client;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.ClientUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientUtilsTest {

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

    final Throwable e1 = assertThrows(IllegalArgumentException.class, () -> ClientUtils.getBlockId(16777216, 0, 0));
    assertTrue(e1.getMessage().contains("Can't support partitionId[16777216], the max value should be 16777215"));

    final Throwable e2 = assertThrows(IllegalArgumentException.class, () -> ClientUtils.getBlockId(0, 2097152, 0));
    assertTrue(e2.getMessage().contains("Can't support taskAttemptId[2097152], the max value should be 2097151"));

    final Throwable e3 = assertThrows(IllegalArgumentException.class, () -> ClientUtils.getBlockId(0, 0, 262144));
    assertTrue(e3.getMessage().contains("Can't support sequence[262144], the max value should be 262143"));
  }
}
