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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.common.TezRuntimeFrameworkConfigs.LOCAL_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssSimpleFetchedInputAllocatorTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(RssSimpleFetchedInputAllocatorTest.class);

  @Test
  public void testAllocate(@TempDir File tmpDir) throws IOException {
    Configuration conf = new Configuration();
    conf.set(LOCAL_DIRS, tmpDir + "/local");

    long jvmMax = 954728448L;
    LOG.info("jvmMax: " + jvmMax);

    float bufferPercent = 0.1f;
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, bufferPercent);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 1.0f);

    long inMemThreshold = (long) (bufferPercent * jvmMax);
    LOG.info("InMemThreshold: " + inMemThreshold);

    RssSimpleFetchedInputAllocator inputManager =
        new RssSimpleFetchedInputAllocator(
            "srcName",
            UUID.randomUUID().toString(),
            123,
            conf,
            Runtime.getRuntime().maxMemory(),
            inMemThreshold,
            "");

    long requestSize = (long) (0.4f * inMemThreshold);
    long compressedSize = 1L;
    LOG.info("RequestSize: " + requestSize);

    FetchedInput fi1 =
        inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(1, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi1.getType());

    FetchedInput fi2 =
        inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(2, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi2.getType());

    // Over limit by this point. Next reserve should give back a DISK allocation
    FetchedInput fi3 =
        inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(3, 1));
    assertEquals(FetchedInput.Type.DISK, fi3.getType());

    // Freed one memory allocation. Next should be mem again.
    fi1.abort();
    fi1.free();
    FetchedInput fi4 =
        inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(4, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi4.getType());

    // Freed one disk allocation. Next sould be disk again (no mem freed)
    fi3.abort();
    fi3.free();
    FetchedInput fi5 =
        inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(4, 1));
    assertEquals(FetchedInput.Type.DISK, fi5.getType());
  }
}
