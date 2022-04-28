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

package com.tencent.rss.common;

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
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    ByteBuffer uncompressData = ByteBuffer.allocateDirect(uncompressLength);
    fastDecompressor.decompress(data, data.position(), uncompressData, 0, uncompressLength);
    return uncompressData;
  }
}
