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

import java.io.IOException;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.util.ChecksumUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezBypassWriterTest {
  @Test
  public void testWrite() {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    ;
    MapOutput mapOutput = MapOutput.createMemoryMapOutput(null, null, 7, true);
    RssTezBypassWriter.write(mapOutput, data);
    byte[] r = mapOutput.getMemory();
    assertTrue(Arrays.equals(data, r));

    mapOutput = MapOutput.createMemoryMapOutput(null, null, 8, true);
    r = mapOutput.getMemory();
    assertFalse(Arrays.equals(data, r));
  }

  @Test
  public void testCalcChecksum() throws IOException {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    byte[] result = new byte[] {-71, -87, 19, -71};
    assertTrue(Arrays.equals(Ints.toByteArray((int) ChecksumUtils.getCrc32(data)), result));
  }
}
