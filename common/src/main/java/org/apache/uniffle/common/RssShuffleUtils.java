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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
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
  
  /**
   * DirectByteBuffers are garbage collected by using a phantom reference and a
   * reference queue. Every once a while, the JVM checks the reference queue and
   * cleans the DirectByteBuffers. However, as this doesn't happen
   * immediately after discarding all references to a DirectByteBuffer, it's
   * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
   * explicitly calls the Cleaner method of a DirectByteBuffer.
   *
   * @param toBeDestroyed
   *          The DirectByteBuffer that will be "cleaned". Utilizes reflection.
   *
   */
  public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed)
          throws IllegalArgumentException, IllegalAccessException,
          InvocationTargetException, SecurityException, NoSuchMethodException {
    Preconditions.checkArgument(toBeDestroyed.isDirect(),
            "toBeDestroyed isn't direct!");
    Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
    cleanerMethod.setAccessible(true);
    Object cleaner = cleanerMethod.invoke(toBeDestroyed);
    Method cleanMethod = cleaner.getClass().getMethod("clean");
    cleanMethod.setAccessible(true);
    cleanMethod.invoke(cleaner);
  }
}
