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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

import java.io.IOException;
import java.io.OutputStream;

// In Tez shuffle, MapOutput encapsulates the logic to fetch map task's output data via http.
// So, in RSS, we should bypass this logic, and directly write data to MapOutput.
public class RssTezBypassWriter {
  private static final Log LOG = LogFactory.getLog(RssTezBypassWriter.class);
  private static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F' , (byte) 0};

  public static void write(MapOutput mapOutput, byte[] buffer) {
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput.getType() == MapOutput.Type.MEMORY) {
      byte[] memory = mapOutput.getMemory();
      System.arraycopy(buffer, 0, memory, 0, buffer.length);
    } else if (mapOutput.getType() == MapOutput.Type.DISK) {
      // RSS leverages its own compression, it is incompatible with hadoop's disk file compression.
      // So we should disable this situation.
      throw new IllegalStateException("RSS does not support OnDiskMapOutput as shuffle ouput,"
              + " try to reduce mapreduce.reduce.shuffle.memory.limit.percent");
    } else {
      throw new IllegalStateException("Merger reserve unknown type of MapOutput: "
              + mapOutput.getClass().getCanonicalName());
    }
  }

  public static void write(FetchedInput mapOutput, byte[] buffer) throws IOException {
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput.getType() == FetchedInput.Type.MEMORY) {
      byte[] memory = ((MemoryFetchedInput) mapOutput).getBytes();
      System.arraycopy(buffer, 0, memory, 0, buffer.length);
    } else if (mapOutput.getType() == FetchedInput.Type.DISK) {
      LOG.info("Write to disk, buffer length:" + buffer.length);
      OutputStream output = ((DiskFetchedInput) mapOutput).getOutputStream();
      output.write(HEADER, 0, HEADER.length);
      output.write(buffer, 0, buffer.length);
      output.flush();
      output.close();
    } else {
      throw new IllegalStateException("Merger reserve unknown type of MapOutput: "
          + mapOutput.getClass().getCanonicalName());
    }
  }
}