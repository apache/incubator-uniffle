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

package org.apache.hadoop.mapred;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SortWriteBufferTest {

  @Test
  public void testReadWrite() throws IOException {

    String keyStr = "key";
    String valueStr = "value";
    BytesWritable key = new BytesWritable(keyStr.getBytes());
    BytesWritable value = new BytesWritable(valueStr.getBytes());
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    Serializer<BytesWritable> keySerializer =  serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer = serializationFactory.getSerializer(BytesWritable.class);
    SortWriteBuffer<BytesWritable, BytesWritable> buffer =
        new SortWriteBuffer<BytesWritable, BytesWritable>(
            1, WritableComparator.get(BytesWritable.class), 1024L);
    keySerializer.open(buffer);
    valSerializer.open(buffer);
    long start = buffer.getDataLength();
    keySerializer.serialize(key);
    valSerializer.serialize(value);
    long end = buffer.getDataLength();
    assertEquals(16, end);
    assertEquals(0, start);
    buffer.addRecord(key, 0, 16);
    assertEquals(16, buffer.getData().length);
    assertEquals(1, buffer.getPartitionId());
    byte[] result = buffer.getData();
    Deserializer<BytesWritable> keyDeserializer = serializationFactory.getDeserializer(BytesWritable.class);
    Deserializer<BytesWritable> valDeserializer = serializationFactory.getDeserializer(BytesWritable.class);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result);
    keyDeserializer.open(byteArrayInputStream);
    valDeserializer.open(byteArrayInputStream);
    BytesWritable keyRead = keyDeserializer.deserialize(null);
    assertEquals(key, keyRead);
    BytesWritable valueRead = keyDeserializer.deserialize(null);
    assertEquals(value, valueRead);

    buffer = new SortWriteBuffer<BytesWritable, BytesWritable>(
        1, WritableComparator.get(BytesWritable.class), 528L);
    keySerializer.open(buffer);
    valSerializer.open(buffer);
    start = buffer.getDataLength();
    assertEquals(0, start);
    keyStr = "key3";
    key = new BytesWritable(keyStr.getBytes());
    keySerializer.serialize(key);
    byte[] valueBytes = new byte[200];
    Map<String, BytesWritable> valueMap = Maps.newConcurrentMap();
    Random random = new Random();
    random.nextBytes(valueBytes);
    value = new BytesWritable(valueBytes);
    valueMap.putIfAbsent(keyStr, value);
    valSerializer.serialize(value);
    end = buffer.getDataLength();
    buffer.addRecord(key, start, end);
    keyStr = "key1";
    key = new BytesWritable(keyStr.getBytes());
    valueBytes = new byte[2032];
    random.nextBytes(valueBytes);
    value = new BytesWritable(valueBytes);
    valueMap.putIfAbsent(keyStr, value);
    start = buffer.getDataLength();
    keySerializer.serialize(key);
    valSerializer.serialize(value);
    end = buffer.getDataLength();
    buffer.addRecord(key, start, end);
    keyStr = "key2";
    key = new BytesWritable(keyStr.getBytes());
    valueBytes = new byte[3100];
    value = new BytesWritable(valueBytes);
    valueMap.putIfAbsent(keyStr, value);
    start = buffer.getDataLength();
    keySerializer.serialize(key);
    valSerializer.serialize(value);
    end = buffer.getDataLength();
    buffer.addRecord(key, start, end);
    result = buffer.getData();
    byteArrayInputStream = new ByteArrayInputStream(result);
    keyDeserializer.open(byteArrayInputStream);
    valDeserializer.open(byteArrayInputStream);
    for (int i = 1; i <= 3; i++) {
      keyRead = keyDeserializer.deserialize(null);
      valueRead = valDeserializer.deserialize(null);
      String tmpStr = "key" + i;
      BytesWritable bytesWritable = new BytesWritable(tmpStr.getBytes());
      assertEquals(bytesWritable, keyRead);
      assertEquals(valueMap.get(tmpStr), valueRead);
    }
  }
}
