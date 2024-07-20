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

package org.apache.uniffle.storage.handler.api;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

public abstract class ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteHandler.class);

  /**
   * Write the blocks to storage
   *
   * @param shuffleBlocks blocks to storage
   * @throws Exception
   */
  public abstract void write(List<ShufflePartitionedBlock> shuffleBlocks) throws Exception;

  /**
   * Write the blocks data to dataWriter, and then write all index to indexWriter
   *
   * @param shuffleBlocks blocks to storage
   * @param dataWriter data writer
   * @param indexWriter index writer
   * @throws Exception
   */
  protected void writeBlocks(
      List<ShufflePartitionedBlock> shuffleBlocks,
      FileWriter dataWriter,
      FileWriter indexWriter) throws Exception {
    long startTime = System.currentTimeMillis();
    FileBasedShuffleSegment[] segments = new FileBasedShuffleSegment[shuffleBlocks.size()];
    // write and flush all block data
    for (int i = 0; i < shuffleBlocks.size(); i++) {
      ShufflePartitionedBlock block = shuffleBlocks.get(i);
      long blockId = block.getBlockId();
      long crc = block.getCrc();
      long startOffset = dataWriter.nextOffset();
      dataWriter.writeData(ByteBufUtils.readBytes(shuffleBlocks.get(i).getData()));
      segments[i] =
          new FileBasedShuffleSegment(
              blockId,
              startOffset,
              block.getLength(),
              block.getUncompressLength(),
              crc,
              block.getTaskAttemptId());
    }
    dataWriter.flush();

    // write and flush all index
    for (FileBasedShuffleSegment segment : segments) {
      indexWriter.writeIndex(segment);
    }
    indexWriter.flush();

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Write handler write {} blocks cost {} ms without file open close",
          shuffleBlocks.size(),
          (System.currentTimeMillis() - startTime));
    }
  }
}
