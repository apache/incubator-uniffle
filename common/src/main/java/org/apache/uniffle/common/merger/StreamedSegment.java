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
import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;

public class StreamedSegment<K, V> extends Segment {

  private RecordsReader<K, V> reader;
  ByteBuf byteBuf = null;

  public StreamedSegment(
      RssConf rssConf,
      PartialInputStream inputStream,
      long blockId,
      Class keyClass,
      Class valueClass,
      boolean raw) {
    super(blockId);
    this.reader = new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, raw);
  }

  public StreamedSegment(
      RssConf rssConf, ByteBuf byteBuf, long blockId, Class keyClass, Class valueClass, boolean raw)
      throws IOException {
    super(blockId);
    this.byteBuf = byteBuf;
    this.byteBuf.retain();
    byte[] buffer = byteBuf.array();
    this.reader =
        new RecordsReader<>(
            rssConf,
            PartialInputStreamImpl.newInputStream(buffer, 0, buffer.length),
            keyClass,
            valueClass,
            raw);
  }

  // The buffer must be sorted by key
  public StreamedSegment(
      RssConf rssConf, byte[] buffer, long blockId, Class keyClass, Class valueClass, boolean raw)
      throws IOException {
    super(blockId);
    this.reader =
        new RecordsReader<>(
            rssConf,
            PartialInputStreamImpl.newInputStream(buffer, 0, buffer.length),
            keyClass,
            valueClass,
            raw);
  }

  public StreamedSegment(
      RssConf rssConf,
      File file,
      long start,
      long end,
      long blockId,
      Class keyClass,
      Class valueClass,
      boolean raw)
      throws IOException {
    super(blockId);
    this.reader =
        new RecordsReader<K, V>(
            rssConf,
            PartialInputStreamImpl.newInputStream(file, start, end),
            keyClass,
            valueClass,
            raw);
  }

  @Override
  public boolean next() throws IOException {
    return this.reader.next();
  }

  @Override
  public Object getCurrentKey() {
    return this.reader.getCurrentKey();
  }

  @Override
  public Object getCurrentValue() {
    return this.reader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    if (byteBuf != null) {
      this.byteBuf.release();
      this.byteBuf = null;
    }
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }
}
