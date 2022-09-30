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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class Lz4Decompressor implements Decompressor {

  private LZ4FastDecompressor decompressor;

  public Lz4Decompressor() {
    this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    decompressor.decompress(src, src.position(), dest, destOffset, uncompressedLen);
  }
}
