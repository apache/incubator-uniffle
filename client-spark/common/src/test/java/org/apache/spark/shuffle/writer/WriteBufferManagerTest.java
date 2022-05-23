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

package org.apache.spark.shuffle.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.Test;

public class WriteBufferManagerTest {

  private WriteBufferManager createManager(SparkConf conf) {
    Serializer kryoSerializer = new KryoSerializer(conf);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager wbm = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        Maps.newHashMap(), mockTaskMemoryManager, new ShuffleWriteMetrics());
    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());
    return spyManager;
  }

  private SparkConf getConf() {
    SparkConf conf = new SparkConf(false);
    conf.set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE, "64")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE, "32")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE, "128")
        .set(RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE, "512")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE, "190");
    return conf;
  }

  @Test
  public void addRecordTest() {
    SparkConf conf = getConf();
    WriteBufferManager wbm = createManager(conf);
    wbm.setShuffleWriteMetrics(new ShuffleWriteMetrics());
    String testKey = "Key";
    String testValue = "Value";
    List<ShuffleBlockInfo> result = wbm.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(32, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    assertEquals(1, wbm.getBuffers().size());
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(0, testKey, testValue);
    result = wbm.addRecord(0, testKey, testValue);
    // single buffer is full
    assertEquals(1, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(96, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
    assertEquals(0, wbm.getBuffers().size());
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    // single buffer is not full, and less than spill size
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(192, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
    assertEquals(3, wbm.getBuffers().size());
    // all buffer size > spill size
    wbm.addRecord(3, testKey, testValue);
    wbm.addRecord(4, testKey, testValue);
    result = wbm.addRecord(5, testKey, testValue);
    assertEquals(6, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(288, wbm.getUsedBytes());
    assertEquals(288, wbm.getInSendListBytes());
    assertEquals(0, wbm.getBuffers().size());
    // free memory
    wbm.freeAllocatedMemory(96);
    assertEquals(416, wbm.getAllocatedBytes());
    assertEquals(192, wbm.getUsedBytes());
    assertEquals(192, wbm.getInSendListBytes());

    assertEquals(11, wbm.getShuffleWriteMetrics().recordsWritten());
    assertTrue(wbm.getShuffleWriteMetrics().bytesWritten() > 0);

    wbm.freeAllocatedMemory(192);
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    result = wbm.clear();
    assertEquals(3, result.size());
    assertEquals(224, wbm.getAllocatedBytes());
    assertEquals(96, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
  }

  @Test
  public void createBlockIdTest() {
    SparkConf conf = getConf();
    WriteBufferManager wbm = createManager(conf);
    WriterBuffer mockWriterBuffer = mock(WriterBuffer.class);
    when(mockWriterBuffer.getData()).thenReturn(new byte[]{});
    when(mockWriterBuffer.getMemoryUsed()).thenReturn(0);
    ShuffleBlockInfo sbi = wbm.createShuffleBlock(0, mockWriterBuffer);
    // seqNo = 0, partitionId = 0, taskId = 0
    assertEquals(0L, sbi.getBlockId());

    // seqNo = 1, partitionId = 0, taskId = 0
    sbi = wbm.createShuffleBlock(0, mockWriterBuffer);
    assertEquals(17592186044416L, sbi.getBlockId());

    // seqNo = 0, partitionId = 1, taskId = 0
    sbi = wbm.createShuffleBlock(1, mockWriterBuffer);
    assertEquals(1048576L, sbi.getBlockId());

    // seqNo = 1, partitionId = 1, taskId = 0
    sbi = wbm.createShuffleBlock(1, mockWriterBuffer);
    assertEquals(17592187092992L, sbi.getBlockId());
  }
}
