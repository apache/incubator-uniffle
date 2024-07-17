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

package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.RawComparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Recordable;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_DEFAULT_MERGED_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergedResultTest {

  private static final int BYTES_LEN = 10240;
  private static final int RECORDS = 1009;
  private static final int SEGMENTS = 4;

  @Test
  public void testMergedResult() throws IOException {
    // 1 Construct cache
    List<Pair<Integer, byte[]>> blocks = new ArrayList<>();
    MergedResult.CacheMergedBlockFuntion cache =
        (byte[] buffer, long blockId, int length) -> {
          assertEquals(blockId - 1, blocks.size());
          blocks.add(Pair.of(length, buffer));
        };

    // 2 Write to merged result
    RssConf rssConf = new RssConf();
    rssConf.set(SERVER_DEFAULT_MERGED_BLOCK_SIZE, String.valueOf(BYTES_LEN / 10));
    MergedResult result = new MergedResult(rssConf, cache, -1);
    OutputStream output = result.getOutputStream();
    for (int i = 0; i < BYTES_LEN; i++) {
      output.write((byte) (i & 0x7F));
      if (output instanceof Recordable) {
        ((Recordable) output).record(i + 1, null, false);
      }
    }
    output.close();

    // 3 check blocks number
    //  Max merged block is 1024, every record have 2 bytes, so will result to 10 block
    assertEquals(10, blocks.size());

    // 4 check the blocks
    int index = 0;
    for (int i = 0; i < blocks.size(); i++) {
      int length = blocks.get(i).getLeft();
      byte[] buffer = blocks.get(i).getRight();
      assertTrue(buffer.length >= length);
      for (int j = 0; j < length; j++) {
        assertEquals(index & 0x7F, buffer[j]);
        index++;
      }
    }
    assertEquals(BYTES_LEN, index);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testMergeSegmentToMergeResult(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);

    // 2 Construct cache
    List<Pair<Integer, byte[]>> blocks = new ArrayList<>();
    MergedResult.CacheMergedBlockFuntion cache =
        (byte[] buffer, long blockId, int length) -> {
          assertEquals(blockId - 1, blocks.size());
          blocks.add(Pair.of(length, buffer));
        };

    // 3 Construct segments, then merge
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
    MergedResult result = new MergedResult(rssConf, cache, -1);
    OutputStream mergedOutputStream = result.getOutputStream();
    Merger.merge(
        rssConf,
        mergedOutputStream,
        segments,
        keyClass,
        valueClass,
        comparator,
        comparator instanceof RawComparator);
    mergedOutputStream.flush();
    mergedOutputStream.close();

    // 4 check merged blocks
    int index = 0;
    for (int i = 0; i < blocks.size(); i++) {
      int length = blocks.get(i).getLeft();
      byte[] buffer = blocks.get(i).getRight();
      RecordsReader reader =
          new RecordsReader(
              rssConf,
              PartialInputStreamImpl.newInputStream(buffer, 0, length),
              keyClass,
              valueClass,
              false);
      while (reader.next()) {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        index++;
      }
      reader.close();
    }
    assertEquals(RECORDS * SEGMENTS, index);
  }
}
