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

package org.apache.uniffle.storage.handler.impl;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.storage.HdfsShuffleHandlerTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HdfsClientReadHandlerTest extends HdfsShuffleHandlerTestBase {

  @Test
  public void test() {
    try {
      String basePath = HDFS_URI + "clientReadTest1";
      HdfsShuffleWriteHandler writeHandler =
          new HdfsShuffleWriteHandler(
              "appId",
              0,
              1,
              1,
              basePath,
              "test",
              conf);

      Map<Long, byte[]> expectedData = Maps.newHashMap();
      Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();

      int readBufferSize = 13;
      int total = 0;
      int totalBlockNum = 0;
      int expectTotalBlockNum = 0;
      for (int i = 0; i < 5; i++) {
        writeHandler.setFailTimes(i);
        int num = new Random().nextInt(17);
        writeTestData(writeHandler,  num, 3, 0, expectedData);
        total += calcExpectedSegmentNum(num, 3, readBufferSize);
        expectTotalBlockNum += num;
        expectedData.forEach((id, block) -> expectBlockIds.addLong(id));
      }

      HdfsClientReadHandler handler = new HdfsClientReadHandler(
          "appId",
          0,
          1,
          1024 * 10214,
          1,
          10,
          readBufferSize,
          expectBlockIds,
          processBlockIds,
          basePath,
          conf);
      Set<Long> actualBlockIds = Sets.newHashSet();

      for (int i = 0; i < total; ++i) {
        ShuffleDataResult shuffleDataResult = handler.readShuffleData();
        totalBlockNum += shuffleDataResult.getBufferSegments().size();
        checkData(shuffleDataResult, expectedData);
        for (BufferSegment bufferSegment : shuffleDataResult.getBufferSegments()) {
          actualBlockIds.add(bufferSegment.getBlockId());
        }
      }

      assertTrue(handler.readShuffleData().isEmpty());
      assertEquals(
          total,
          handler.getHdfsShuffleFileReadHandlers()
              .stream()
              .mapToInt(i -> i.getShuffleDataSegments().size())
              .sum());
      assertEquals(expectTotalBlockNum, totalBlockNum);
      assertEquals(expectedData.keySet(), actualBlockIds);
      assertEquals(5, handler.getReadHandlerIndex());
      handler.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
