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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.IdUtils;
import org.apache.tez.common.ShuffleAssignmentsInfoWritable;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RssUnorderedKVOutputTest {
  private static Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
  private Configuration conf;
  private FileSystem localFs;
  private Path workingDir;

  @BeforeEach
  public void setup() throws IOException {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    workingDir = new Path(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir", "/tmp")),
        RssOrderedPartitionedKVOutputTest.class.getName()).makeQualified(
        localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
    conf.set(RSS_AM_SHUFFLE_MANAGER_ADDRESS, "localhost");
    conf.setInt(RSS_AM_SHUFFLE_MANAGER_PORT, 0);
    conf.setInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, 0);
    conf.setInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, 1);
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
    RssUnorderedKVOutput output = new RssUnorderedKVOutput(outputContext, numPartitions);
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
    byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload
        .getEmptyPartitions());
    BitSet emptyPartionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    assertEquals(numPartitions, emptyPartionsBitSet.cardinality());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(emptyPartionsBitSet.get(i));
    }
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testClose() throws Exception {
    try (MockedStatic<RPC> rpc = Mockito.mockStatic(RPC.class);) {
      TezRemoteShuffleUmbilicalProtocol protocol = mock(TezRemoteShuffleUmbilicalProtocol.class);
      GetShuffleServerResponse response = new GetShuffleServerResponse();
      ShuffleAssignmentsInfo shuffleAssignmentsInfo = new ShuffleAssignmentsInfo(new HashMap(), new HashMap());
      response.setShuffleAssignmentsInfoWritable(new ShuffleAssignmentsInfoWritable(shuffleAssignmentsInfo));
      doReturn(response).when(protocol).getShuffleAssignments(any());
      rpc.when(() -> RPC.getProxy(any(), anyLong(), any(), any())).thenReturn(protocol);
      try (MockedStatic<IdUtils> idUtils = Mockito.mockStatic(IdUtils.class)) {
        idUtils.when(() -> IdUtils.getApplicationAttemptId()).thenReturn(OutputTestHelpers.APP_ATTEMPT_ID);
        idUtils.when(() -> IdUtils.getAppAttemptId()).thenReturn(OutputTestHelpers.APP_ATTEMPT_ID.getAttemptId());
        try (MockedStatic<ConverterUtils> converterUtils = Mockito.mockStatic(ConverterUtils.class)) {
          ContainerId containerId = ContainerId.newContainerId(OutputTestHelpers.APP_ATTEMPT_ID, 1);
          converterUtils.when(() -> ConverterUtils.toContainerId(null)).thenReturn(containerId);
          converterUtils.when(() -> ConverterUtils.toContainerId(anyString())).thenReturn(containerId);
          OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
          int numPartitions = 1;
          RssUnorderedKVOutput output = new RssUnorderedKVOutput(outputContext, numPartitions);
          output.initialize();
          output.start();
          Assertions.assertNotNull(output.getWriter());
          output.close();
        }
      }
    }
  }
}
