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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.IFileOutputStream;

import org.apache.uniffle.common.exception.RssException;

// In MR shuffle, MapOutput encapsulates the logic to fetch map task's output data via http.
// So, in RSS, we should bypass this logic, and directly write data to MapOutput.
public class RssBypassWriter {
  private static final Log LOG = LogFactory.getLog(RssBypassWriter.class);

  public static void write(MapOutput mapOutput, byte[] buffer, CompressionCodec codec) {
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput instanceof InMemoryMapOutput) {
      InMemoryMapOutput inMemoryMapOutput = (InMemoryMapOutput) mapOutput;
      // In InMemoryMapOutput constructor method, we create a decompressor or borrow a decompressor
      // from
      // pool. Now we need to put it back, otherwise we will create a decompressor for every
      // InMemoryMapOutput
      // object, they will cause `out of direct memory` problems.
      CodecPool.returnDecompressor(getDecompressor(inMemoryMapOutput));
      write(inMemoryMapOutput, buffer);
    } else if (mapOutput instanceof OnDiskMapOutput) {
      write((OnDiskMapOutput) mapOutput, buffer, codec);
    } else {
      throw new IllegalStateException(
          "Merger reserve unknown type of MapOutput: " + mapOutput.getClass().getCanonicalName());
    }
  }

  private static void write(InMemoryMapOutput inMemoryMapOutput, byte[] buffer) {
    byte[] memory = inMemoryMapOutput.getMemory();
    System.arraycopy(buffer, 0, memory, 0, buffer.length);
  }

  // The parameter 'codec' is hadoop codec. RssFetcher will fetch compressed data
  // (compressed by uiffle), then decompressed to the parameter 'buffer'. Until now
  // buffer is the decompressed data. But when MergeManagerImpl merge the temporary
  // shuffle data, will use hadoop compression code. So we should compressed the buffer
  // for future merge.
  // For now, it is not recommended to enable "mapreduce.map.output.compress".
  private static void write(
      OnDiskMapOutput onDiskMapOutput, byte[] buffer, CompressionCodec codec) {
    OutputStream disk = null;
    try {
      Class clazz = Class.forName(OnDiskMapOutput.class.getName());
      Field diskField = clazz.getDeclaredField("disk");
      diskField.setAccessible(true);
      disk = (OutputStream) diskField.get(onDiskMapOutput);
    } catch (Exception e) {
      throw new RssException(
          "Failed to access OnDiskMapOutput by reflection due to: " + e.getMessage());
    }
    if (disk == null) {
      throw new RssException("OnDiskMapOutput should not contain null disk stream");
    }

    // Copy data to local-disk
    FSDataOutputStream out = null;
    CompressionOutputStream compressedOut = null;
    IFileOutputStream ifos = null;
    Compressor compressor = null;
    try {
      ifos = new IFileOutputStream(disk);
      if (codec != null) {
        compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          compressor.reset();
          compressedOut = codec.createOutputStream(ifos, compressor);
          out = new FSDataOutputStream(compressedOut, null);
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          out = new FSDataOutputStream(ifos, null);
        }
      } else {
        out = new FSDataOutputStream(ifos, null);
      }
      out.write(buffer, 0, buffer.length);
      out.close();
    } catch (IOException ioe) {
      throw new RssException("Failed to write OnDiskMapOutput due to: " + ioe.getMessage());
    } finally {
      // Close the streams
      if (out != null) {
        IOUtils.cleanup(LOG, out);
      } else if (compressedOut != null) {
        IOUtils.cleanup(LOG, compressedOut);
      } else if (ifos != null) {
        IOUtils.cleanup(LOG, ifos);
      } else {
        IOUtils.cleanup(LOG, disk);
      }
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }
    }
  }

  static Decompressor getDecompressor(InMemoryMapOutput inMemoryMapOutput) {
    try {
      Class clazz = Class.forName(InMemoryMapOutput.class.getName());
      Field deCompressorField = clazz.getDeclaredField("decompressor");
      deCompressorField.setAccessible(true);
      Decompressor decompressor = (Decompressor) deCompressorField.get(inMemoryMapOutput);
      return decompressor;
    } catch (Exception e) {
      throw new RssException("Get Decompressor fail " + e.getMessage());
    }
  }
}
