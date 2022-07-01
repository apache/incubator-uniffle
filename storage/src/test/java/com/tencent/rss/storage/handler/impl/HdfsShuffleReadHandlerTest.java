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

package com.tencent.rss.storage.handler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.storage.HdfsShuffleHandlerTestBase;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class HdfsShuffleReadHandlerTest extends HdfsShuffleHandlerTestBase {

  @Test
  public void test() {
    try {
      String basePath = HDFS_URI + "HdfsShuffleFileReadHandlerTest";
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

      int readBufferSize = 13;
      int totalBlockNum = 0;
      int expectTotalBlockNum = new Random().nextInt(37);
      int blockSize = new Random().nextInt(7) + 1;
      writeTestData(writeHandler, expectTotalBlockNum, blockSize, 0, expectedData);
      int total = calcExpectedSegmentNum(expectTotalBlockNum, blockSize, readBufferSize);
      Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap processBlockIds =  Roaring64NavigableMap.bitmapOf();
      expectedData.forEach((id, block) -> expectBlockIds.addLong(id));
      String fileNamePrefix = ShuffleStorageUtils.getFullShuffleDataFolder(basePath,
          ShuffleStorageUtils.getShuffleDataPathWithRange("appId",
              0, 1, 1, 10)) + "/test_0";
      HdfsShuffleReadHandler handler =
          new HdfsShuffleReadHandler("appId", 0, 1, fileNamePrefix,
              readBufferSize, expectBlockIds, processBlockIds, conf);

      Set<Long> actualBlockIds = Sets.newHashSet();
      for (int i = 0; i < total; ++i) {
        ShuffleDataResult shuffleDataResult = handler.readShuffleData();
        totalBlockNum += shuffleDataResult.getBufferSegments().size();
        checkData(shuffleDataResult, expectedData);
        for (BufferSegment bufferSegment : shuffleDataResult.getBufferSegments()) {
          actualBlockIds.add(bufferSegment.getBlockId());
        }
      }

      assertNull(handler.readShuffleData());
      assertEquals(
          total,
          handler.getShuffleDataSegments().size());
      assertEquals(expectTotalBlockNum, totalBlockNum);
      assertEquals(expectedData.keySet(), actualBlockIds);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
