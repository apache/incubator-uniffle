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

import java.nio.ByteBuffer;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.apache.uniffle.common.config.RssClientConf.ZSTD_COMPRESSION_LEVEL;

public class Codec implements Compressor, Decompressor {

  private final RssConf rssConf;
  private volatile Decompressor decompressor;
  private volatile Compressor compressor;

  private Codec(RssConf rssConf) {
    this.rssConf = rssConf;
  }

  public static Codec getInstance(RssConf rssConf) {
    return new Codec(rssConf);
  }

  public enum Type {
    LZ4,
    ZSTD,
    NOOP,
  }

  @Override
  public byte[] compress(byte[] src) {
    return getOrCreateCompressor().compress(src);
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    getOrCreateDeCompressor().decompress(src, uncompressedLen, dest, destOffset);
  }

  private Decompressor getOrCreateDeCompressor() {
    if (decompressor == null) {
      synchronized (decompressor) {
        if (decompressor == null) {
          CompressionFactory.Type type = rssConf.get(COMPRESSION_TYPE);
          switch (type) {
            case ZSTD:
              this.decompressor = new ZstdDecompressor();
              break;
            case NOOP:
              this.decompressor = new NoOpDecompressor();
              break;
            case LZ4:
            default:
              this.decompressor = new Lz4Decompressor();
          }
        }
      }
    }
    return decompressor;
  }

  private Compressor getOrCreateCompressor() {
    if (compressor == null) {
      synchronized (compressor) {
        if (compressor == null) {
          CompressionFactory.Type type = rssConf.get(COMPRESSION_TYPE);
          switch (type) {
            case ZSTD:
              this.compressor = new ZstdCompressor(rssConf.get(ZSTD_COMPRESSION_LEVEL));
              break;
            case NOOP:
              this.compressor = new NoOpCompressor();
              break;
            case LZ4:
            default:
              this.compressor = new Lz4Compressor();
          }
        }
      }
    }
    return compressor;
  }

}
