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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Recordable;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE;

public class MergedResult {

  private final RssConf rssConf;
  private final long mergedBlockSize;
  // raw offset by blockId
  private final List<Long> offsets = new ArrayList<>();
  private final CacheMergedBlockFuntion cachedMergedBlock;

  public MergedResult(
      RssConf rssConf, CacheMergedBlockFuntion cachedMergedBlock, int mergedBlockSize) {
    this.rssConf = rssConf;
    this.cachedMergedBlock = cachedMergedBlock;
    this.mergedBlockSize =
        mergedBlockSize > 0
            ? mergedBlockSize
            : this.rssConf.getSizeAsBytes(
                SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE.key(),
                SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE.defaultValue());
    offsets.add(0L);
  }

  public OutputStream getOutputStream() {
    return new MergedSegmentOutputStream();
  }

  public boolean isOutOfBound(long blockId) {
    return blockId >= offsets.size();
  }

  public long getBlockSize(long blockId) {
    return offsets.get((int) blockId) - offsets.get((int) (blockId - 1));
  }

  @FunctionalInterface
  public interface CacheMergedBlockFuntion {
    void cache(byte[] buffer, long blockId, int length);
  }

  class MergedSegmentOutputStream extends OutputStream implements Recordable {

    ByteArrayOutputStream current;

    MergedSegmentOutputStream() {
      current = new ByteArrayOutputStream((int) mergedBlockSize);
    }

    @Override
    public void write(int b) throws IOException {
      current.write(b);
    }

    @Override
    public void close() throws IOException {
      if (current != null) {
        current.close();
        current = null;
      }
    }

    @Override
    public boolean record(long written, Flushable flushable, boolean force) throws IOException {
      assert written >= 0;
      long currentOffsetInThisBlock = written - offsets.get(offsets.size() - 1);
      if (currentOffsetInThisBlock >= mergedBlockSize || (currentOffsetInThisBlock > 0 && force)) {
        if (flushable != null) {
          flushable.flush();
        }
        cachedMergedBlock.cache(
            current.toByteArray(), offsets.size(), (int) (currentOffsetInThisBlock));
        offsets.add(written);
        if (!force) {
          current = new ByteArrayOutputStream((int) mergedBlockSize);
        }
        return true;
      }
      return false;
    }
  }
}
