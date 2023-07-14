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

package org.apache.tez.runtime.library.output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssUnorderedPartitionedKVOutputTest {
  private static Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
  private Configuration conf;
  private FileSystem localFs;
  private Path workingDir;

  @BeforeEach
  public void setup() throws IOException {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    workingDir =
        new Path(
                System.getProperty("test.build.data", System.getProperty("java.io.tmpdir", "/tmp")),
                RssOrderedPartitionedKVOutputTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
  }

  @AfterEach
  public void cleanup() throws IOException {
    localFs.delete(workingDir, true);
  }

  @Test
  @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
  public void testNonStartedOutput() throws Exception {
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
    int numPartitions = 10;
    RssUnorderedPartitionedKVOutput output =
        new RssUnorderedPartitionedKVOutput(outputContext, numPartitions);
    List<Event> events = output.close();
    assertEquals(2, events.size());
    Event event1 = events.get(0);
    assertTrue(event1 instanceof VertexManagerEvent);
    Event event2 = events.get(1);
    assertTrue(event2 instanceof CompositeDataMovementEvent);
    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) event2;
    ByteBuffer bb = cdme.getUserPayload();
    ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(bb));
    assertTrue(shufflePayload.hasEmptyPartitions());
    byte[] emptyPartitions =
        TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions());
    BitSet emptyPartionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    assertEquals(numPartitions, emptyPartionsBitSet.cardinality());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(emptyPartionsBitSet.get(i));
    }
  }
}
