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

package org.apache.uniffle.common.serializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WritableSerializerTest {

  private static final int LOOP = 1009;
  private static RssConf rssConf = new RssConf();

  // Test 1: both write and read will use common api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file"
      })
  public void testSerDeKeyValues1(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.data"))
            : new ByteArrayOutputStream();
    SerializationStream serializationStream = instance.serializeStream(outputStream, false);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      serializationStream.writeRecord(genData(keyClass, i), genData(valueClass, i));
      offsets[i] = serializationStream.getTotalBytesWritten();
    }
    serializationStream.close();

    // 3 Random read
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      PartialInputStream inputStream =
          isFileMode
              ? PartialInputStream.newInputStream(new File(tmpDir, "tmp.data"), off, Long.MAX_VALUE)
              : PartialInputStream.newInputStream(
                  ByteBuffer.wrap(((ByteArrayOutputStream) outputStream).toByteArray()),
                  off,
                  ((ByteArrayOutputStream) outputStream).size());
      DeserializationStream deserializationStream =
          instance.deserializeStream(inputStream, keyClass, valueClass, false);
      for (int j = i + 1; j < LOOP; j++) {
        assertTrue(deserializationStream.nextRecord());
        assertEquals(genData(keyClass, j), deserializationStream.getCurrentKey());
        assertEquals(genData(valueClass, j), deserializationStream.getCurrentValue());
      }
      deserializationStream.close();
    }
  }

  // Test 2: write with common api, read with raw api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file"
      })
  public void testSerDeKeyValues2(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.data"))
            : new ByteArrayOutputStream();
    SerializationStream serializationStream = instance.serializeStream(outputStream, false);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      serializationStream.writeRecord(genData(keyClass, i), genData(valueClass, i));
      offsets[i] = serializationStream.getTotalBytesWritten();
    }
    serializationStream.close();

    // 3 Random read
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      PartialInputStream inputStream =
          isFileMode
              ? PartialInputStream.newInputStream(new File(tmpDir, "tmp.data"), off, Long.MAX_VALUE)
              : PartialInputStream.newInputStream(
                  ByteBuffer.wrap(((ByteArrayOutputStream) outputStream).toByteArray()),
                  off,
                  ((ByteArrayOutputStream) outputStream).size());

      DeserializationStream deserializationStream =
          instance.deserializeStream(inputStream, keyClass, valueClass, true);
      for (int j = i + 1; j < LOOP; j++) {
        assertTrue(deserializationStream.nextRecord());
        DataOutputBuffer keyBuffer = (DataOutputBuffer) deserializationStream.getCurrentKey();
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
        assertEquals(genData(keyClass, j), instance.deserialize(keyInputBuffer, keyClass));
        DataOutputBuffer valueBuffer = (DataOutputBuffer) deserializationStream.getCurrentValue();
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
        assertEquals(genData(valueClass, j), instance.deserialize(valueInputBuffer, valueClass));
      }
      deserializationStream.close();
    }
  }

  // Test 3: write with raw api, read with common api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file"
      })
  public void testSerDeKeyValues3(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.data"))
            : new ByteArrayOutputStream();
    SerializationStream serializationStream = instance.serializeStream(outputStream, true);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      DataOutputBuffer valueBuffer = new DataOutputBuffer();
      instance.serialize(genData(keyClass, i), keyBuffer);
      instance.serialize(genData(valueClass, i), valueBuffer);
      serializationStream.writeRecord(keyBuffer, valueBuffer);
      offsets[i] = serializationStream.getTotalBytesWritten();
    }
    serializationStream.close();

    // 3 Random read
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      PartialInputStream inputStream =
          isFileMode
              ? PartialInputStream.newInputStream(new File(tmpDir, "tmp.data"), off, Long.MAX_VALUE)
              : PartialInputStream.newInputStream(
                  ByteBuffer.wrap(((ByteArrayOutputStream) outputStream).toByteArray()),
                  off,
                  ((ByteArrayOutputStream) outputStream).size());
      DeserializationStream deserializationStream =
          instance.deserializeStream(inputStream, keyClass, valueClass, false);
      for (int j = i + 1; j < LOOP; j++) {
        assertTrue(deserializationStream.nextRecord());
        assertEquals(genData(keyClass, j), deserializationStream.getCurrentKey());
        assertEquals(genData(valueClass, j), deserializationStream.getCurrentValue());
      }
      deserializationStream.close();
    }
  }

  // Test 4: both write and read use raw api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file"
      })
  public void testSerDeKeyValues4(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.data"))
            : new ByteArrayOutputStream();
    SerializationStream serializationStream = instance.serializeStream(outputStream, true);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      DataOutputBuffer valueBuffer = new DataOutputBuffer();
      instance.serialize(genData(keyClass, i), keyBuffer);
      instance.serialize(genData(valueClass, i), valueBuffer);
      serializationStream.writeRecord(keyBuffer, valueBuffer);
      offsets[i] = serializationStream.getTotalBytesWritten();
    }
    serializationStream.close();

    // 3 Random read
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      PartialInputStream inputStream =
          isFileMode
              ? PartialInputStream.newInputStream(new File(tmpDir, "tmp.data"), off, Long.MAX_VALUE)
              : PartialInputStream.newInputStream(
                  ByteBuffer.wrap(((ByteArrayOutputStream) outputStream).toByteArray()),
                  off,
                  ((ByteArrayOutputStream) outputStream).size());

      DeserializationStream deserializationStream =
          instance.deserializeStream(inputStream, keyClass, valueClass, true);
      for (int j = i + 1; j < LOOP; j++) {
        assertTrue(deserializationStream.nextRecord());
        DataOutputBuffer keyBuffer = (DataOutputBuffer) deserializationStream.getCurrentKey();
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
        assertEquals(genData(keyClass, j), instance.deserialize(keyInputBuffer, keyClass));
        DataOutputBuffer valueBuffer = (DataOutputBuffer) deserializationStream.getCurrentValue();
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
        assertEquals(genData(valueClass, j), instance.deserialize(valueInputBuffer, valueClass));
      }
      deserializationStream.close();
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {Text.class, IntWritable.class})
  public void testSerDeObject(Class aClass) throws Exception {
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();
    int number = new Random().nextInt(99999);
    DataOutputBuffer output = new DataOutputBuffer();
    instance.serialize(genData(aClass, number), output);
    DataInputBuffer input = new DataInputBuffer();
    input.reset(output.getData(), 0, output.getData().length);
    Object obj = instance.deserialize(input, aClass);
    assertEquals(genData(aClass, number), obj);
  }
}
