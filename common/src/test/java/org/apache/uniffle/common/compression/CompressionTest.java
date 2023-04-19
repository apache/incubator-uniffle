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
import java.util.ArrayList;
import java.util.List;

import com.github.luben.zstd.Zstd;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.xerial.snappy.pure.SnappyRawCompressor;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class CompressionTest {

  static List<Arguments> testCompression() {
    int[] sizes = {1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024};
    Codec.Type[] types = {Codec.Type.ZSTD, Codec.Type.LZ4, Codec.Type.SNAPPY, Codec.Type.NOOP};

    List<Arguments> arguments = new ArrayList<>();
    for (int size : sizes) {
      for (Codec.Type type : types) {
        arguments.add(
            Arguments.of(size, type)
        );
      }
    }
    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  public void testCompression(int size, Codec.Type type) {
    byte[] data = RandomUtils.nextBytes(size);
    RssConf conf = new RssConf();
    conf.set(COMPRESSION_TYPE, type);
        
    // case1: heap bytebuffer
    Codec codec = Codec.newInstance(conf);
    byte[] compressed = codec.compress(data);

    ByteBuffer dest = ByteBuffer.allocate(size);
    codec.decompress(ByteBuffer.wrap(compressed), size, dest, 0);

    assertArrayEquals(data, dest.array());

    // case2: non-heap bytebuffer
    ByteBuffer src = ByteBuffer.allocateDirect(compressed.length);
    src.put(compressed);
    src.flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(size);
    codec.decompress(src, size, dst, 0);
    byte[] res = new byte[size];
    dst.get(res);
    assertArrayEquals(data, res);

    // case3: use the recycled bytebuffer
    ByteBuffer recycledDst = ByteBuffer.allocate(size + 10);
    codec.decompress(ByteBuffer.wrap(compressed), size, recycledDst, 0);
    recycledDst.get(res);
    assertArrayEquals(data, res);

    // case4: use bytebuffer compress
    ByteBuffer srcBuffer = ByteBuffer.allocateDirect(size);
    srcBuffer.put(data);
    srcBuffer.flip();
    ByteBuffer destBuffer = ByteBuffer.allocateDirect(maxCompressedLength(type, size));
    codec.compress(srcBuffer, destBuffer);

    destBuffer.flip();
    srcBuffer.clear();
    codec.decompress(destBuffer, size, srcBuffer, 0);
    byte[] res2 = new byte[size];
    srcBuffer.get(res2);
    assertArrayEquals(data, res2);
  }

  private int maxCompressedLength(Codec.Type type, int sourceLength) {
    if (type == Codec.Type.LZ4) {
      return sourceLength + sourceLength / 255 + 16;
    } else if (type == Codec.Type.SNAPPY) {
      return SnappyRawCompressor.maxCompressedLength(sourceLength);
    } else if (type == Codec.Type.ZSTD) {
      return (int) Zstd.compressBound(sourceLength);
    } else if (type == Codec.Type.NOOP) {
      return sourceLength;
    } else {
      return sourceLength * 2 + 32;
    }
  }
}
