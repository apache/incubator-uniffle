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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.RawComparator;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RssSortedGroupedMergedInputTest {
  MergedInputContext createMergedInputContext() {
    return mock(MergedInputContext.class);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSimpleConcatenatedMergedKeyValueInput() throws Exception {

    DummyInput sInput1 = new DummyInput(10);
    DummyInput sInput2 = new DummyInput(10);
    DummyInput sInput3 = new DummyInput(10);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);
    MergedInputContext mockContext = createMergedInputContext();
    ConcatenatedMergedKeyValueInput input =
        new ConcatenatedMergedKeyValueInput(mockContext, sInputs);

    KeyValueReader kvReader = input.getReader();
    int keyCount = 0;
    while (kvReader.next()) {
      keyCount++;
      Integer key = (Integer) kvReader.getCurrentKey();
      Integer value = (Integer) kvReader.getCurrentValue();
    }
    assertTrue(keyCount == 30);
    // one for each reader change and one to exit
    verify(mockContext, times(4)).notifyProgress();

    getNextFromFinishedReader(kvReader);
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSimpleConcatenatedMergedKeyValuesInput() throws Exception {
    SortedTestKeyValuesReader kvsReader1 =
        new SortedTestKeyValuesReader(new int[] {1, 2, 3}, new int[][] {{1, 1}, {2, 2}, {3, 3}});

    SortedTestKeyValuesReader kvsReader2 =
        new SortedTestKeyValuesReader(new int[] {1, 2, 3}, new int[][] {{1, 1}, {2, 2}, {3, 3}});

    SortedTestKeyValuesReader kvsReader3 =
        new SortedTestKeyValuesReader(new int[] {1, 2, 3}, new int[][] {{1, 1}, {2, 2}, {3, 3}});

    SortedTestInput sInput1 = new SortedTestInput(kvsReader1);
    SortedTestInput sInput2 = new SortedTestInput(kvsReader2);
    SortedTestInput sInput3 = new SortedTestInput(kvsReader3);

    List<Input> sInputs = new LinkedList<Input>();
    sInputs.add(sInput1);
    sInputs.add(sInput2);
    sInputs.add(sInput3);
    MergedInputContext mockContext = createMergedInputContext();
    ConcatenatedMergedKeyValuesInput input =
        new ConcatenatedMergedKeyValuesInput(mockContext, sInputs);

    KeyValuesReader kvsReader = input.getReader();
    int keyCount = 0;
    while (kvsReader.next()) {
      keyCount++;
      Integer key = (Integer) kvsReader.getCurrentKey();
      Iterator<Object> valuesIter = kvsReader.getCurrentValues().iterator();
      int valCount = 0;
      while (valuesIter.hasNext()) {
        valCount++;
        Integer val = (Integer) valuesIter.next();
      }
      assertEquals(2, valCount);
    }
    assertEquals(9, keyCount);
    // one for each reader change and one to exit
    verify(mockContext, times(4)).notifyProgress();

    getNextFromFinishedReader(kvsReader);
  }

  private void getNextFromFinishedReader(KeyValuesReader kvsReader) {
    // Try reading again and it should throw IOException
    try {
      boolean hasNext = kvsReader.next();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("For usage, please refer to"));
    }
  }

  private void getNextFromFinishedReader(KeyValueReader kvReader) {
    // Try reading again and it should throw IOException
    try {
      boolean hasNext = kvReader.next();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("For usage, please refer to"));
    }
  }

  private static class SortedTestInput extends RssOrderedGroupedKVInput {

    final SortedTestKeyValuesReader reader;

    SortedTestInput(SortedTestKeyValuesReader reader) {
      super(null, 0);
      this.reader = reader;
    }

    @Override
    public List<Event> initialize() throws IOException {
      return null;
    }

    @Override
    public void start() throws IOException {}

    @Override
    public KeyValuesReader getReader() throws IOException {
      return reader;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) {}

    @Override
    public List<Event> close() throws IOException {
      return null;
    }

    @SuppressWarnings("rawtypes")
    public RawComparator getInputKeyComparator() {
      return new RawComparatorForTest();
    }
  }

  private static class SortedTestKeyValuesReader extends KeyValuesReader {

    final int[] keys;
    final int[][] values;
    int currentIndex = -1;

    SortedTestKeyValuesReader(int[] keys, int[][] vals) {
      this.keys = keys;
      this.values = vals;
    }

    @Override
    public boolean next() throws IOException {
      hasCompletedProcessing();
      currentIndex++;
      if (keys == null || currentIndex >= keys.length) {
        completedProcessing = true;
        return false;
      }
      return true;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return keys[currentIndex];
    }

    @Override
    public Iterable<Object> getCurrentValues() throws IOException {
      List<Object> ints = new LinkedList<Object>();
      for (int i = 0; i < values[currentIndex].length; i++) {
        ints.add(Integer.valueOf(values[currentIndex][i]));
      }
      return ints;
    }
  }

  private static class DummyInput implements Input {
    DummyKeyValueReader reader;

    DummyInput(int records) {
      reader = new DummyKeyValueReader(records);
    }

    @Override
    public void start() throws Exception {}

    @Override
    public Reader getReader() throws Exception {
      return reader;
    }
  }

  private static class DummyKeyValueReader extends KeyValueReader {
    private int records;

    DummyKeyValueReader(int records) {
      this.records = records;
    }

    @Override
    public boolean next() throws IOException {
      return (records-- > 0);
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return records;
    }

    @Override
    public Object getCurrentValue() throws IOException {
      return records;
    }
  }

  private static class RawComparatorForTest implements RawComparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
      return o1 - o2;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      throw new UnsupportedOperationException();
    }
  }
}
