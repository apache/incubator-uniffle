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

package org.apache.uniffle.common.compression;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.apache.uniffle.common.config.RssClientConf.ZSTD_COMPRESSION_LEVEL;

public class CompressionFactory {

  public enum Type {
    LZ4,
    ZSTD,
    NOOP,
  }

  private CompressionFactory() {
    // ignore
  }

  private static class LazyHolder {
    static final CompressionFactory INSTANCE = new CompressionFactory();
  }

  public static CompressionFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  public Compressor getCompressor(RssConf conf) {
    Type type = conf.get(COMPRESSION_TYPE);
    switch (type) {
      case ZSTD:
        return new ZstdCompressor(conf.get(ZSTD_COMPRESSION_LEVEL));
      case NOOP:
        return new NoOpCompressor();
      case LZ4:
      default:
        return new Lz4Compressor();
    }
  }

  public Decompressor getDecompressor(RssConf conf) {
    Type type = conf.get(COMPRESSION_TYPE);
    switch (type) {
      case ZSTD:
        return new ZstdDecompressor();
      case NOOP:
        return new NoOpDecompressor();
      case LZ4:
      default:
        return new Lz4Decompressor();
    }
  }
}
