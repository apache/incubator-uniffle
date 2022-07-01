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

package org.apache.uniffle.common;

import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static byte[] compressData(byte[] data) {
    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    return compressor.compress(data);
  }

  public static byte[] decompressData(byte[] data, int uncompressLength) {
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    byte[] uncompressData = new byte[uncompressLength];
    fastDecompressor.decompress(data, 0, uncompressData, 0, uncompressLength);
    return uncompressData;
  }

  public static ByteBuffer decompressData(ByteBuffer data, int uncompressLength) {
    return decompressData(data, uncompressLength, true);
  }

  public static ByteBuffer decompressData(ByteBuffer data, int uncompressLength, boolean useDirectMem) {
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    ByteBuffer uncompressData;
    if (useDirectMem) {
      uncompressData = ByteBuffer.allocateDirect(uncompressLength);
    } else {
      uncompressData = ByteBuffer.allocate(uncompressLength);
    }
    fastDecompressor.decompress(data, data.position(), uncompressData, 0, uncompressLength);
    return uncompressData;
  }
}
