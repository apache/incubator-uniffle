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

package org.apache.uniffle.shuffle.partition;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.shuffle.RssFlinkApplication;
import org.apache.uniffle.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.shuffle.resource.DefaultRssShuffleResource;
import org.apache.uniffle.shuffle.resource.RssShuffleResource;
import org.apache.uniffle.shuffle.resource.RssShuffleResourceDescriptor;
import org.apache.uniffle.shuffle.writer.RssShuffleOutputGate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RssShuffleResultPartitionTest {

  private RssFlinkApplication rssFlinkApplication;

  private NetworkBufferPool globalPool;

  private JobID jobID = new JobID(10, 1);

  private static final int TOTAL_BUFFERS = 1000;

  private static final int TOTAL_BUFFER_SIZE = 1024;

  private static final int BUFFER_SIZE = 1024;

  private static final int numSubs = 1;

  private static final int EMPTY_BUFFER_SIZE = Integer.MAX_VALUE;

  @BeforeEach
  public void setUp() {
    globalPool = new NetworkBufferPool(TOTAL_BUFFERS, TOTAL_BUFFER_SIZE);
    rssFlinkApplication = new RssFlinkApplication();
  }

  @Test
  public void testWriteLargeRecord() throws Exception {
    int numBuffers = 10;
    int numSubpartitions = 1;

    BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
    RssShuffleResultPartition partition =
        genRssShuffleResultPartition(numSubpartitions, bufferPool);
    partition.setup();

    ByteBuffer recordWritten = generateRandomData(BUFFER_SIZE * numBuffers, new Random());
    partition.emitRecord(recordWritten, 0);
  }

  private ByteBuffer generateRandomData(int dataSize, Random random) {
    byte[] dataWritten = new byte[dataSize];
    random.nextBytes(dataWritten);
    return ByteBuffer.wrap(dataWritten);
  }

  private RssShuffleResultPartition genRssShuffleResultPartition(
      int numSubpartitions, BufferPool bufferPool) throws Exception {
    BufferPool bufferPool2 = globalPool.createBufferPool(10, 200);
    RssShuffleOutputGate outputGate = createOutputGate(bufferPool2);
    return new RssShuffleResultPartition(
        "RssShuffleResultPartitionTest",
        0,
        new ResultPartitionID(),
        ResultPartitionType.BLOCKING,
        numSubpartitions,
        numSubpartitions,
        BUFFER_SIZE,
        new ResultPartitionManager(),
        null,
        () -> bufferPool,
        outputGate);
  }

  private RssShuffleOutputGate createOutputGate(BufferPool bufferPool) throws Exception {
    ResultPartitionID resultPartitionID = new ResultPartitionID();

    RssShuffleResourceDescriptor resourceDescriptor = genRssShuffleResourceDescriptor();

    ShuffleServerInfo ssi = new ShuffleServerInfo("127.0.0.1", 0);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(0, Lists.newArrayList(ssi));

    RssShuffleResource shuffleResource =
        new DefaultRssShuffleResource(partitionToServers, resourceDescriptor);

    RssShuffleDescriptor shuffleDescriptor =
        new RssShuffleDescriptor(resultPartitionID, jobID, shuffleResource);

    RssShuffleOutputGate outputGate =
        new RssShuffleOutputGate(
            shuffleDescriptor,
            numSubs,
            EMPTY_BUFFER_SIZE,
            () -> bufferPool,
            new Configuration(),
            1,
            genShuffleWriteClientImplTest());
    return outputGate;
  }

  private RssShuffleResourceDescriptor genRssShuffleResourceDescriptor() {
    int uniffleShuffleId = rssFlinkApplication.getUniffleShuffleId(jobID.toString());
    RssShuffleResourceDescriptor descriptor =
        new RssShuffleResourceDescriptor(uniffleShuffleId, 0, 0, 0);
    return descriptor;
  }

  private ShuffleWriteClientImpl genShuffleWriteClientImplTest() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterRequestTimeSec(10)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());

    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(new RssSendShuffleDataResponse(StatusCode.SUCCESS));
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS));

    return spyClient;
  }
}
