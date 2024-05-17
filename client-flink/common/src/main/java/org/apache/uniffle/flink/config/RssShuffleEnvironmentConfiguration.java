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

package org.apache.uniffle.flink.config;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

public class RssShuffleEnvironmentConfiguration {
  // Flink Configuration
  private Configuration configuration;
  private int pageSize;
  private ResultPartitionManager resultPartitionManager;
  private NetworkBufferPool networkBufferPool;

  public RssShuffleEnvironmentConfiguration() {}

  public static RssShuffleEnvironmentConfiguration fromConfiguration(
      ShuffleEnvironmentContext shuffleEnvironmentContext) {
    Configuration configuration = shuffleEnvironmentContext.getConfiguration();
    final int pageSize = ConfigurationParserUtils.getPageSize(configuration);
    final MemorySize networkMemorySize = shuffleEnvironmentContext.getNetworkMemorySize();
    final int numberOfNetworkBuffers =
        calculateNumberOfNetworkBuffers(configuration, networkMemorySize, pageSize);

    Duration requestSegmentsTimeout =
        Duration.ofMillis(
            configuration.getLong(
                NettyShuffleEnvironmentOptions
                    .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));

    return null;
  }

  private static int calculateNumberOfNetworkBuffers(
      Configuration configuration, MemorySize networkMemorySize, int pageSize) {

    // tolerate offcuts between intended and allocated memory due to segmentation (will be
    // available to the user-space memory)
    long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
    if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "The given number of memory bytes ("
              + networkMemorySize.getBytes()
              + ") corresponds to more than MAX_INT pages.");
    }
    return (int) numberOfNetworkBuffersLong;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public ResultPartitionManager getResultPartitionManager() {
    return resultPartitionManager;
  }

  public void setResultPartitionManager(ResultPartitionManager resultPartitionManager) {
    this.resultPartitionManager = resultPartitionManager;
  }

  public NetworkBufferPool getNetworkBufferPool() {
    return networkBufferPool;
  }

  public void setNetworkBufferPool(NetworkBufferPool networkBufferPool) {
    this.networkBufferPool = networkBufferPool;
  }
}
