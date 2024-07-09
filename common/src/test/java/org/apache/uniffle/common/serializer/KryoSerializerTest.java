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
import java.util.Random;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerializerUtils.SomeClass;
import org.apache.uniffle.common.serializer.kryo.KryoSerializer;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KryoSerializerTest {

  private static final int LOOP = 1009;
  private static RssConf rssConf = new RssConf();

  @ParameterizedTest
  @ValueSource(classes = {ByteArrayOutputStream.class, FileOutputStream.class})
  void testKryoWriteRandomRead(Class<?> streamClass, @TempDir File tmpDir) throws Exception {
    boolean isFileMode = streamClass.getName().equals(FileOutputStream.class.getName());
    Kryo kryo = new Kryo();
    // 1 Write object to stream
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.kryo"))
            : new ByteArrayOutputStream();
    Output output = new Output(outputStream);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      SomeClass object = (SomeClass) SerializerUtils.genData(SomeClass.class, i);
      kryo.writeObject(output, object);
      offsets[i] = output.total();
    }
    output.close();

    // 2 Read object from every offset
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      Input input =
          isFileMode
              ? new Input(
                  PartialInputStreamImpl.newInputStream(
                      new File(tmpDir, "tmp.kryo"), off, Long.MAX_VALUE))
              : new Input(
                  PartialInputStreamImpl.newInputStream(
                      ((ByteArrayOutputStream) outputStream).toByteArray(), off, Long.MAX_VALUE));
      for (int j = i + 1; j < LOOP; j++) {
        SomeClass object = kryo.readObject(input, SomeClass.class);
        assertEquals(SerializerUtils.genData(SomeClass.class, j), object);
      }
      input.close();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "java.lang.String,java.lang.Integer,mem",
        "java.lang.String,java.lang.Integer,file",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,int,mem",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,int,file"
      })
  public void testSerDeKeyValues(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");

    KryoSerializer serializer = new KryoSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream =
        isFileMode
            ? new FileOutputStream(new File(tmpDir, "tmp.kryo"))
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
      PartialInputStreamImpl inputStream =
          isFileMode
              ? PartialInputStreamImpl.newInputStream(
                  new File(tmpDir, "tmp.kryo"), off, Long.MAX_VALUE)
              : PartialInputStreamImpl.newInputStream(
                  ((ByteArrayOutputStream) outputStream).toByteArray(), off, Long.MAX_VALUE);
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

  @ParameterizedTest
  @ValueSource(
      classes = {java.lang.String.class, java.lang.Integer.class, SomeClass.class, int.class})
  public void testSerDeObject(Class aClass) throws Exception {
    KryoSerializer serializer = new KryoSerializer(rssConf);
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
