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

package org.apache.spark.shuffle.reader;

import static org.junit.Assert.assertEquals;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssShuffleUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

public abstract class AbstractRssReaderTest extends HdfsTestBase {

  private AtomicInteger atomicInteger = new AtomicInteger(0);

  protected void validateResult(Iterator iterator,
      Map<String, String> expectedData, int recordNum) {
    Set<String> actualKeys = Sets.newHashSet();
    while (iterator.hasNext()) {
      Product2 product2 = (Product2) iterator.next();
      String key = (String) product2._1();
      String value = (String) product2._2();
      actualKeys.add(key);
      assertEquals(expectedData.get(key), value);
    }
    assertEquals(recordNum, actualKeys.size());
    assertEquals(expectedData.keySet(), actualKeys);
  }

  protected void writeTestData(
      ShuffleWriteHandler handler,
      int blockNum,
      int recordNum,
      Map<String, String> expectedData,
      Roaring64NavigableMap blockIdBitmap,
      String keyPrefix,
      Serializer serializer,
      int partitionID) throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    SerializerInstance serializerInstance = serializer.newInstance();
    for (int i = 0; i < blockNum; i++) {
      Output output = new Output(1024, 2048);
      SerializationStream serializeStream = serializerInstance.serializeStream(output);
      for (int j = 0; j < recordNum; j++) {
        String key = keyPrefix + "_" + i + "_" + j;
        String value = "valuePrefix_" + i + "_" + j;
        expectedData.put(key, value);
        writeData(serializeStream, key, value);
      }
      long blockId = ClientUtils.getBlockId(partitionID, 0, atomicInteger.getAndIncrement());
      blockIdBitmap.add(blockId);
      blocks.add(createShuffleBlock(output.toBytes(), blockId));
      serializeStream.close();
    }
    handler.write(blocks);
  }

  protected ShufflePartitionedBlock createShuffleBlock(byte[] data, long blockId) {
    byte[] compressData = RssShuffleUtils.compressData(data);
    long crc = ChecksumUtils.getCrc32(compressData);
    return new ShufflePartitionedBlock(compressData.length, data.length, crc, blockId, 0,
        compressData);
  }

  protected void writeData(SerializationStream serializeStream, String key, String value) {
    serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    serializeStream.flush();
  }
}
