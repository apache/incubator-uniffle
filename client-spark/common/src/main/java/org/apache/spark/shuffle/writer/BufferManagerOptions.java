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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferManagerOptions {

  private static final Logger LOG = LoggerFactory.getLogger(BufferManagerOptions.class);

  private long bufferSize;
  private long serializerBufferSize;
  private long bufferSegmentSize;
  private long bufferSpillThreshold;
  private long preAllocatedBufferSize;
  private long requireMemoryInterval;
  private int requireMemoryRetryMax;

  public BufferManagerOptions(SparkConf sparkConf) {
    bufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE);
    serializerBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE_DEFAULT_VALUE);
    bufferSegmentSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE_DEFAULT_VALUE);
    bufferSpillThreshold = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE);
    preAllocatedBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE_DEFAULT_VALUE);
    requireMemoryInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_REQUIRE_MEMORY_INTERVAL,
        RssClientConfig.RSS_WRITER_REQUIRE_MEMORY_INTERVAL_DEFAULT_VALUE);
    requireMemoryRetryMax = sparkConf.getInt(RssClientConfig.RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX,
        RssClientConfig.RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX_DEFAULT_VALUE);
    LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SIZE + "=" + bufferSize);
    LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE + "=" + bufferSpillThreshold);
    LOG.info(RssClientConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE + "=" + preAllocatedBufferSize);
    checkBufferSize();
  }

  private void checkBufferSize() {
    if (bufferSize < 0) {
      throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SIZE
          + "=" + bufferSize);
    }
    if (bufferSpillThreshold < 0) {
      throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE
          + "=" + bufferSpillThreshold);
    }
    if (bufferSegmentSize > bufferSize) {
      LOG.warn(RssClientConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE + "[" + bufferSegmentSize + "] should be less than "
          + RssClientConfig.RSS_WRITER_BUFFER_SIZE + "[" + bufferSize + "]");
    }
  }

  // limit of buffer size is 2G
  public int getBufferSize() {
    return parseToInt(bufferSize);
  }

  public int getSerializerBufferSize() {
    return parseToInt(serializerBufferSize);
  }

  public int getBufferSegmentSize() {
    return parseToInt(bufferSegmentSize);
  }

  private int parseToInt(long value) {
    if (value > Integer.MAX_VALUE) {
      value = Integer.MAX_VALUE;
    }
    return (int) value;
  }

  public long getPreAllocatedBufferSize() {
    return preAllocatedBufferSize;
  }

  public long getBufferSpillThreshold() {
    return bufferSpillThreshold;
  }

  public long getRequireMemoryInterval() {
    return requireMemoryInterval;
  }

  public int getRequireMemoryRetryMax() {
    return requireMemoryRetryMax;
  }
}
