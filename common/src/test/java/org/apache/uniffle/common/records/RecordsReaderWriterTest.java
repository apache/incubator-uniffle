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

package org.apache.uniffle.common.records;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RecordsReaderWriterTest {

  private static final int RECORDS = 1009;
  private static final int LOOP = 5;

  // Test 1: both write and read will use common api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      })
  public void testWriteAndReadRecordFile1(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");

    // 2 Write
    long[] offsets = new long[RECORDS];
    OutputStream outputStream =
        isFileMode ? new FileOutputStream(tmpFile) : new ByteArrayOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass, false);
    for (int i = 0; i < RECORDS; i++) {
      writer.append(SerializerUtils.genData(keyClass, i), SerializerUtils.genData(valueClass, i));
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    PartialInputStreamImpl inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, 0, tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(), 0, Long.MAX_VALUE);
    RecordsReader reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, offsets[RECORDS - 1], tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(),
                offsets[RECORDS - 1],
                Long.MAX_VALUE);
    reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? PartialInputStreamImpl.newInputStream(tmpFile, offset, tmpFile.length())
              : PartialInputStreamImpl.newInputStream(
                  ((ByteArrayOutputStream) outputStream).toByteArray(), offset, Long.MAX_VALUE);
      reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
      while (reader.next()) {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        index++;
      }
      assertEquals(RECORDS, index);
    }
    reader.close();
  }

  // Test 2: write with common api, read with raw api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      })
  public void testWriteAndReadRecordFile2(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    long[] offsets = new long[RECORDS];
    OutputStream outputStream =
        isFileMode ? new FileOutputStream(tmpFile) : new ByteArrayOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass, false);
    for (int i = 0; i < RECORDS; i++) {
      writer.append(SerializerUtils.genData(keyClass, i), SerializerUtils.genData(valueClass, i));
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    PartialInputStreamImpl inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, 0, tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(), 0, Long.MAX_VALUE);
    RecordsReader reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
    int index = 0;
    while (reader.next()) {
      DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
      DataInputBuffer keyInputBuffer = new DataInputBuffer();
      keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
      assertEquals(
          SerializerUtils.genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
      DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
      DataInputBuffer valueInputBuffer = new DataInputBuffer();
      valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
      assertEquals(
          SerializerUtils.genData(valueClass, index),
          instance.deserialize(valueInputBuffer, valueClass));
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, offsets[RECORDS - 1], tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(),
                offsets[RECORDS - 1],
                Long.MAX_VALUE);
    reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? PartialInputStreamImpl.newInputStream(tmpFile, offset, tmpFile.length())
              : PartialInputStreamImpl.newInputStream(
                  ((ByteArrayOutputStream) outputStream).toByteArray(), offset, Long.MAX_VALUE);
      reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
      while (reader.next()) {
        DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(keyClass, index),
            instance.deserialize(keyInputBuffer, keyClass));
        DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(valueClass, index),
            instance.deserialize(valueInputBuffer, valueClass));
        index++;
      }
      assertEquals(RECORDS, index);
    }
    reader.close();
  }

  // Test 3: write with raw api, read with common api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      })
  public void testWriteAndReadRecordFile3(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    long[] offsets = new long[RECORDS];
    OutputStream outputStream =
        isFileMode ? new FileOutputStream(tmpFile) : new ByteArrayOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass, true);
    for (int i = 0; i < RECORDS; i++) {
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      DataOutputBuffer valueBuffer = new DataOutputBuffer();
      instance.serialize(genData(keyClass, i), keyBuffer);
      instance.serialize(genData(valueClass, i), valueBuffer);
      writer.append(keyBuffer, valueBuffer);
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    PartialInputStreamImpl inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, 0, tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(), 0, Long.MAX_VALUE);
    RecordsReader reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, offsets[RECORDS - 1], tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(),
                offsets[RECORDS - 1],
                Long.MAX_VALUE);
    reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? PartialInputStreamImpl.newInputStream(tmpFile, offset, tmpFile.length())
              : PartialInputStreamImpl.newInputStream(
                  ((ByteArrayOutputStream) outputStream).toByteArray(), offset, Long.MAX_VALUE);
      reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, false);
      while (reader.next()) {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        index++;
      }
      assertEquals(RECORDS, index);
    }
    reader.close();
  }

  // Test 4: both write and read use raw api
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      })
  public void testWriteAndReadRecordFile4(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    long[] offsets = new long[RECORDS];
    OutputStream outputStream =
        isFileMode ? new FileOutputStream(tmpFile) : new ByteArrayOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass, true);
    for (int i = 0; i < RECORDS; i++) {
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      DataOutputBuffer valueBuffer = new DataOutputBuffer();
      instance.serialize(genData(keyClass, i), keyBuffer);
      instance.serialize(genData(valueClass, i), valueBuffer);
      writer.append(keyBuffer, valueBuffer);
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    PartialInputStreamImpl inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, 0, tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(), 0, Long.MAX_VALUE);
    RecordsReader reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
    int index = 0;
    while (reader.next()) {
      DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
      DataInputBuffer keyInputBuffer = new DataInputBuffer();
      keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
      assertEquals(
          SerializerUtils.genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
      DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
      DataInputBuffer valueInputBuffer = new DataInputBuffer();
      valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
      assertEquals(
          SerializerUtils.genData(valueClass, index),
          instance.deserialize(valueInputBuffer, valueClass));
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? PartialInputStreamImpl.newInputStream(tmpFile, offsets[RECORDS - 1], tmpFile.length())
            : PartialInputStreamImpl.newInputStream(
                ((ByteArrayOutputStream) outputStream).toByteArray(),
                offsets[RECORDS - 1],
                Long.MAX_VALUE);
    reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? PartialInputStreamImpl.newInputStream(tmpFile, offset, tmpFile.length())
              : PartialInputStreamImpl.newInputStream(
                  ((ByteArrayOutputStream) outputStream).toByteArray(), offset, Long.MAX_VALUE);
      reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass, true);
      while (reader.next()) {
        DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(keyClass, index),
            instance.deserialize(keyInputBuffer, keyClass));
        DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(valueClass, index),
            instance.deserialize(valueInputBuffer, valueClass));
        index++;
      }
      assertEquals(RECORDS, index);
    }
    reader.close();
  }
}
