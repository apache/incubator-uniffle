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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.RawComparator;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortWriteBuffer<K, V> extends OutputStream  {

  private static final Logger LOG = LoggerFactory.getLogger(SortWriteBuffer.class);
  private long copyTime = 0;
  private final List<WrappedBuffer> buffers = Lists.newArrayList();
  private final List<Record<K>> records = Lists.newArrayList();
  private int dataLength = 0;
  private int totalKeyLength = 0;
  private long sortTime = 0;
  private final RawComparator<K> comparator;
  private long maxSegmentSize;
  private int partitionId;

  public SortWriteBuffer(int partitionId, RawComparator<K> comparator, long maxSegmentSize) {
    this.partitionId = partitionId;
    this.comparator = comparator;
    this.maxSegmentSize = maxSegmentSize;
  }

  public synchronized long addRecord(K key, long start, long end) {
    long keyLength = GraphLayout.parseInstance(key).totalSize();
    records.add(new Record<K>(key, start, end));
    totalKeyLength += keyLength;
    return keyLength;
  }

  public synchronized byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long startSort = System.currentTimeMillis();
    records.sort(new Comparator<Record<K>>() {
      @Override
      public int compare(Record<K> o1, Record<K> o2) {
        return comparator.compare(o1.getKey(), o2.getKey());
      }
    });

    long startCopy =  System.currentTimeMillis();
    sortTime += startCopy - startSort;
    for (Record<K> record : records) {
      int beginIndex = (int) (record.getStart() / maxSegmentSize);
      int beginOffSet = (int) (record.getStart() % maxSegmentSize);
      int endIndex = (int) (record.getEnd() / maxSegmentSize);
      int endOffset = (int) (record.getEnd() % maxSegmentSize);
      if (beginIndex == endIndex) {
        int length = endOffset - beginOffSet;
        System.arraycopy(buffers.get(beginIndex).getBuffer(), beginOffSet, data, offset, length);
        offset += length;
      } else {
        int finalBeginOffset = beginOffSet;
        for (int j = beginIndex; j <= endIndex; j++) {
          int finalEndOffset = (int) ((j == endIndex) ? endOffset : maxSegmentSize);
          int length = finalEndOffset - finalBeginOffset;
          System.arraycopy(buffers.get(j).getBuffer(), finalBeginOffset, data, offset, length);
          offset += length;
          finalBeginOffset = 0;
        }
      }
    }

    copyTime += System.currentTimeMillis() - startCopy;
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public int getTotalKeyLength() {
    return totalKeyLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public long getSortTime() {
    return sortTime;
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public void write(int b) throws IOException {
    if (dataLength + 4 > buffers.size() * maxSegmentSize) {
      buffers.add(new WrappedBuffer((int) maxSegmentSize));
    }
    int index = (int) (dataLength / maxSegmentSize);
    int offset = (int) (dataLength % maxSegmentSize);
    WrappedBuffer buffer = buffers.get(index);
    buffer.getBuffer()[offset] = (byte) b;
    dataLength++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0)
        || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    int bufferNum = (int)((dataLength + len) / maxSegmentSize) + 1 - buffers.size();
    for (int i = 0; i < bufferNum; i++) {
      buffers.add(new WrappedBuffer((int) maxSegmentSize));
    }
    int index = (int) (dataLength / maxSegmentSize);
    int offset = (int) (dataLength % maxSegmentSize);
    int srcPos = 0;
    while (len > 0) {
      int copyLength = 0;
      if (offset + len > maxSegmentSize) {
        copyLength = (int) (maxSegmentSize - offset);
      } else {
        copyLength = len;
      }
      System.arraycopy(b, srcPos, buffers.get(index).getBuffer(), offset, copyLength);
      offset = 0;
      srcPos += copyLength;
      index++;
      len -= copyLength;
      dataLength += copyLength;
    }
  }

  private static final class Record<K> {

    private K key;
    private long start;
    private long end;

    Record(K key, long start, long end) {
      this.key = key;
      this.start = start;
      this.end = end;
    }

    public K getKey() {
      return key;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }

  private static final class WrappedBuffer {

    private byte[] buffer;
    private int size;

    WrappedBuffer(int size) {
      this.buffer = new byte[size];
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }

}
