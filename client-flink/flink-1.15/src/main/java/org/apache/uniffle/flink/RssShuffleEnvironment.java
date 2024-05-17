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

package org.apache.uniffle.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.flink.config.RssShuffleEnvironmentConfiguration;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.*;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class RssShuffleEnvironment
    implements ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleEnvironment.class);

  protected final Object lock = new Object();
  protected boolean isClosed;

  /** Network buffer pool for shuffle read and shuffle write. */
  protected NetworkBufferPool networkBufferPool;

  /** A trivial {@link ResultPartitionManager}. */
  protected ResultPartitionManager resultPartitionManager;

  protected Configuration config;

  protected int networkBufferSize;

  public RssShuffleEnvironment(RssShuffleEnvironmentConfiguration rssConfiguration) {
    this.networkBufferPool = rssConfiguration.getNetworkBufferPool();
    this.resultPartitionManager = rssConfiguration.getResultPartitionManager();
    this.config = rssConfiguration.getConfiguration();
    this.networkBufferSize = rssConfiguration.getPageSize();
    this.isClosed = false;
  }

  @Override
  public int start() throws IOException {
    synchronized (lock) {
      checkState(!isClosed, "The RemoteShuffleEnvironment has already been shut down.");
      LOG.info("Starting the network environment and its components.");
      return 1;
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Create Output Writers and Input Readers
  // --------------------------------------------------------------------------------------------

  @Override
  public ShuffleIOOwnerContext createShuffleIOOwnerContext(
      String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
    MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
    return new ShuffleIOOwnerContext(
        checkNotNull(ownerName),
        checkNotNull(executionAttemptID),
        parentGroup,
        nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
        nettyGroup.addGroup(METRIC_GROUP_INPUT));
  }

  /**
   * Factory method for the {@link ResultPartitionWriter ResultPartitionWriters} to produce result
   * partitions.
   *
   * <p>The order of the {@link ResultPartitionWriter ResultPartitionWriters} in the returned
   * collection should be the same as the iteration order of the passed {@code
   * resultPartitionDeploymentDescriptors}.
   *
   * @param ownerContext the owner context relevant for partition creation
   * @param resultPartitionDeploymentDescriptors descriptors of the partition, produced by the owner
   * @return list of the {@link ResultPartitionWriter ResultPartitionWriters}
   */
  @Override
  public List<ResultPartitionWriter> createResultPartitionWriters(
      ShuffleIOOwnerContext ownerContext,
      List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
    synchronized (lock) {
      checkState(!isClosed, "The RssShuffleEnvironment has already been shut down.");

      ResultPartitionWriter[] resultPartitions =
          new ResultPartitionWriter[resultPartitionDeploymentDescriptors.size()];

      for (int partitionIndex = 0; partitionIndex < resultPartitions.length; partitionIndex++) {

        String ownerName = ownerContext.getOwnerName();

        ResultPartitionDeploymentDescriptor descriptor =
            resultPartitionDeploymentDescriptors.get(partitionIndex);
        RssShuffleDescriptor shuffleDescriptor =
            (RssShuffleDescriptor) descriptor.getShuffleDescriptor();
        ResultPartitionID resultPartitionId = shuffleDescriptor.getResultPartitionID();
        ResultPartitionType partitionType = descriptor.getPartitionType();
        int numberOfSubpartitions = descriptor.getNumberOfSubpartitions();
        int totalNumberOfPartitions = descriptor.getTotalNumberOfPartitions();
        int maxParallelism = descriptor.getMaxParallelism();

        String compressionCodec =
            this.config.getString(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);
        BufferCompressor compressor = getBufferCompressor(compressionCodec);

        resultPartitions[partitionIndex] =
            new RssShuffleResultPartition(
                ownerName,
                partitionIndex,
                resultPartitionId,
                partitionType,
                numberOfSubpartitions,
                totalNumberOfPartitions,
                maxParallelism,
                this.networkBufferSize,
                this.resultPartitionManager,
                compressor,
                null,
                shuffleDescriptor,
                this.config);
      }
      return Arrays.asList(resultPartitions);
    }
  }

  private BufferCompressor getBufferCompressor(String compressionCodec) {
    if (StringUtils.isNotBlank(compressionCodec)) {
      new BufferCompressor(networkBufferSize, compressionCodec);
    }
    return null;
  }

  @Override
  public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  @Override
  public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
    return new ArrayList<>();
  }

  @Override
  public List<IndexedInputGate> createInputGates(
      ShuffleIOOwnerContext ownerContext,
      PartitionProducerStateProvider partitionProducerStateProvider,
      List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

    synchronized (lock) {
      checkState(!isClosed, "The RemoteShuffleEnvironment has already been shut down.");

      IndexedInputGate[] inputGates = new IndexedInputGate[inputGateDeploymentDescriptors.size()];
      for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
        InputGateDeploymentDescriptor igdd = inputGateDeploymentDescriptors.get(gateIndex);
        String ownerName = ownerContext.getOwnerName();

        IndexedInputGate inputGate =
            new RssShuffleInputGate(ownerName, gateIndex, igdd, null, null, 0, this.config, null);

        inputGates[gateIndex] = inputGate;
      }
      return Arrays.asList(inputGates);
    }
  }

  @Override
  public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
      throws IOException, InterruptedException {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  @Override
  public void close() throws Exception {}
}
