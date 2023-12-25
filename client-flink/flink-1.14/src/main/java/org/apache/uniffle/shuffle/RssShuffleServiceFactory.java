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

package org.apache.uniffle.shuffle;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

public class RssShuffleServiceFactory
    implements ShuffleServiceFactory<
        RssShuffleDescriptor, ResultPartitionWriter, IndexedInputGate> {
  @Override
  public ShuffleMaster<RssShuffleDescriptor> createShuffleMaster(
      ShuffleMasterContext shuffleMasterContext) {
    return new RssShuffleMaster(shuffleMasterContext);
  }

  @Override
  public ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> createShuffleEnvironment(
      ShuffleEnvironmentContext context) {
    Configuration configuration = context.getConfiguration();
    int bufferSize = ConfigurationParserUtils.getPageSize(configuration);
    int numBuffers = (int) (context.getNetworkMemorySize().getBytes() / bufferSize);
    ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
    MetricGroup metricGroup = context.getParentMetricGroup();
    Duration requestSegmentsTimeout =
        Duration.ofMillis(
            configuration.getLong(
                NettyShuffleEnvironmentOptions
                    .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
    NetworkBufferPool networkBufferPool =
        new NetworkBufferPool(numBuffers, bufferSize, requestSegmentsTimeout);
    return null;
  }
}
