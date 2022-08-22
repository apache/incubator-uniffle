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

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssShuffleUtilsTest {

  @ParameterizedTest
  @ValueSource(ints = {1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024})
  public void testCompression(int size) {
    byte[] data = RandomUtils.nextBytes(size);
    byte[] compressed = RssShuffleUtils.compressData(data);
    byte[] decompressed = RssShuffleUtils.decompressData(compressed, size);
    assertArrayEquals(data, decompressed);

    ByteBuffer decompressedBB = RssShuffleUtils.decompressData(ByteBuffer.wrap(compressed), size);
    byte[] buffer = new byte[size];
    decompressedBB.get(buffer);
    assertArrayEquals(data, buffer);

    ByteBuffer decompressedBB2 = RssShuffleUtils.decompressData(ByteBuffer.wrap(compressed), size, false);
    byte[] buffer2 = new byte[size];
    decompressedBB2.get(buffer2);
    assertArrayEquals(data, buffer2);
  }

  @Test
  public void testDestroyDirectByteBuffer() throws Exception {
    int size = 10;
    byte b = 1;
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
    for (int i = 0; i < size; i++) {
      byteBuffer.put(b);
    }
    byteBuffer.flip();
    RssShuffleUtils.destroyDirectByteBuffer(byteBuffer);
    // The memory may not be released fast enough.
    Thread.sleep(200);
    boolean same = true;
    byte[] read = new byte[size];
    byteBuffer.get(read);
    for (byte br : read) {
      if (b != br) {
        same = false;
        break;
      }
    }
    assertTrue(!same);
  }
}
