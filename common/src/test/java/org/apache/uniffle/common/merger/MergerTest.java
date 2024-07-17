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

package org.apache.uniffle.common.merger;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergerTest {

  private static final int RECORDS = 1009;
  private static final int SEGMENTS = 4;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testMergeSegmentToFile(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);

    // 2 Construct segments, then merge
    RssConf rssConf = new RssConf();
    List<Segment> segments = new ArrayList<>();
    Comparator comparator = SerializerUtils.getComparator(keyClass);
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        segments.add(
            SerializerUtils.genMemorySegment(
                rssConf,
                keyClass,
                valueClass,
                i,
                i,
                SEGMENTS,
                RECORDS,
                comparator instanceof RawComparator));
      } else {
        segments.add(
            SerializerUtils.genFileSegment(
                rssConf,
                keyClass,
                valueClass,
                i,
                i,
                SEGMENTS,
                RECORDS,
                tmpDir,
                comparator instanceof RawComparator));
      }
    }
    File mergedFile = new File(tmpDir, "data.merged");
    FileOutputStream outputStream = new FileOutputStream(mergedFile);
    Merger.merge(
        rssConf,
        outputStream,
        segments,
        keyClass,
        valueClass,
        comparator,
        comparator instanceof RawComparator);
    outputStream.close();

    // 3 Check the merged file
    RecordsReader reader =
        new RecordsReader(
            rssConf,
            PartialInputStreamImpl.newInputStream(mergedFile, 0, mergedFile.length()),
            keyClass,
            valueClass,
            false);
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS * SEGMENTS, index);
    reader.close();
  }
}
