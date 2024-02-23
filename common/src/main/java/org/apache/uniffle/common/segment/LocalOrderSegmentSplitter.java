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

package org.apache.uniffle.common.segment;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockId;

/**
 * {@class LocalOrderSegmentSplitter} will be initialized only when the {@class
 * ShuffleDataDistributionType} is LOCAL_ORDER, which means the index file will be split into
 * several segments according to its locally ordered properties. And it will skip some blocks, but
 * the remaining blocks in a segment are continuous.
 *
 * <p>This strategy will be useful for Spark AQE skew optimization, it will split the single
 * partition into multiple shuffle readers, and each one will fetch partial single partition data
 * which is in the range of [StartMapIndex, endMapIndex). And so if one reader uses this, it will
 * skip lots of unnecessary blocks.
 *
 * <p>Last but not least, this split strategy depends on LOCAL_ORDER of index file, which must be
 * guaranteed by the shuffle server.
 */
public class LocalOrderSegmentSplitter implements SegmentSplitter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalOrderSegmentSplitter.class);

  private Roaring64NavigableMap expectTaskIds;
  private int readBufferSize;

  public LocalOrderSegmentSplitter(Roaring64NavigableMap expectTaskIds, int readBufferSize) {
    this.expectTaskIds = expectTaskIds;
    this.readBufferSize = readBufferSize;
  }

  @Override
  public List<ShuffleDataSegment> split(ShuffleIndexResult shuffleIndexResult) {
    if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
      return Lists.newArrayList();
    }

    ByteBuffer indexData = shuffleIndexResult.getIndexData();
    long dataFileLen = shuffleIndexResult.getDataFileLen();

    List<BufferSegment> bufferSegments = Lists.newArrayList();

    List<ShuffleDataSegment> dataFileSegments = Lists.newArrayList();
    int bufferOffset = 0;
    long fileOffset = -1;
    long totalLen = 0;

    long lastExpectedBlockIndex = -1;

    List<Long> indexTaskIds = new ArrayList<>();

    /**
     * One ShuffleDataSegment should meet following requirements:
     *
     * <p>1. taskId in [startMapIndex, endMapIndex) taskIds bitmap. Attention: the index in the
     * range is not the map task id, which means the required task ids are not continuous. 2.
     * ShuffleDataSegment size should < readBufferSize 3. Single shuffleDataSegment's blocks should
     * be continuous
     */
    int index = 0;
    while (indexData.hasRemaining()) {
      try {
        long offset = indexData.getLong();
        int length = indexData.getInt();
        int uncompressLength = indexData.getInt();
        long crc = indexData.getLong();
        long blockId = indexData.getLong();
        long taskAttemptId = indexData.getLong();

        totalLen += length;
        indexTaskIds.add(taskAttemptId);

        // If ShuffleServer is flushing the file at this time, the length in the index file record
        // may be greater
        // than the length in the actual data file, and it needs to be returned at this time to
        // avoid EOFException
        if (dataFileLen != -1 && totalLen > dataFileLen) {
          LOGGER.info(
              "Abort inconsistent data, the data length: {}(bytes) recorded in index file is greater than "
                  + "the real data file length: {}(bytes). Partition id: {}. This should not happen. "
                  + "This may happen when the data is flushing, please ignore.",
              totalLen,
              dataFileLen,
              BlockId.getPartitionId(blockId));
          break;
        }

        boolean conditionOfDiscontinuousBlocks =
            lastExpectedBlockIndex != -1
                && bufferSegments.size() > 0
                && expectTaskIds.contains(taskAttemptId)
                && index - lastExpectedBlockIndex != 1;

        boolean conditionOfLimitedBufferSize = bufferOffset >= readBufferSize;

        if (conditionOfDiscontinuousBlocks || conditionOfLimitedBufferSize) {
          ShuffleDataSegment sds = new ShuffleDataSegment(fileOffset, bufferOffset, bufferSegments);
          dataFileSegments.add(sds);
          bufferSegments = Lists.newArrayList();
          bufferOffset = 0;
          fileOffset = -1;
        }

        if (expectTaskIds.contains(taskAttemptId)) {
          if (fileOffset == -1) {
            fileOffset = offset;
          }
          bufferSegments.add(
              new BufferSegment(
                  blockId, bufferOffset, length, uncompressLength, crc, taskAttemptId));
          bufferOffset += length;
          lastExpectedBlockIndex = index;
        }
        index++;
      } catch (BufferUnderflowException ue) {
        throw new RssException("Read index data under flow", ue);
      }
    }

    if (bufferOffset > 0) {
      ShuffleDataSegment sds = new ShuffleDataSegment(fileOffset, bufferOffset, bufferSegments);
      dataFileSegments.add(sds);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Index file task-ids sequence: {}, expected task-ids: {}",
          indexTaskIds,
          getExpectedTaskIds(expectTaskIds));
    }
    return dataFileSegments;
  }

  private List<Long> getExpectedTaskIds(Roaring64NavigableMap expectTaskIds) {
    List<Long> taskIds = new ArrayList<>();
    expectTaskIds.forEach(value -> taskIds.add(value));
    return taskIds;
  }
}
