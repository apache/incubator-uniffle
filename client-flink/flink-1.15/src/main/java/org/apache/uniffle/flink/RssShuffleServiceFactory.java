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

import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.flink.config.RssShuffleEnvironmentConfiguration;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.flink.shuffle.RssShuffleMaster;

/**
 * Uniffle Rss based shuffle service implementation. When implementing it, we mainly referred to the
 * implementation of {@link NettyShuffleServiceFactory}
 */
public class RssShuffleServiceFactory
    implements ShuffleServiceFactory<
        RssShuffleDescriptor, ResultPartitionWriter, IndexedInputGate> {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServiceFactory.class);

  /**
   * Factory method to create a specific {@link ShuffleMaster} implementation. ShuffleMaster will be
   * created on the JM(JobMaster) side.
   *
   * @param shuffleMasterContext shuffle context for shuffle master.
   * @return shuffle manager implementation
   */
  @Override
  public ShuffleMaster<RssShuffleDescriptor> createShuffleMaster(
      ShuffleMasterContext shuffleMasterContext) {
    LOG.info("RssShuffleServiceFactory createShuffleMaster.");
    return new RssShuffleMaster(shuffleMasterContext);
  }

  /**
   * Factory method to create a specific local {@link ShuffleEnvironment} implementation.
   * ShuffleEnvironment will be created on the TE (TaskExecutor) side.
   *
   * @param shuffleEnvironmentContext local context
   * @return local shuffle service environment implementation
   */
  @Override
  public ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> createShuffleEnvironment(
      ShuffleEnvironmentContext shuffleEnvironmentContext) {
    LOG.info("RssShuffleServiceFactory createShuffleEnvironment.");
    // Parse the parameters required to create RssShuffleResultPartition and RssShuffleInputGate
    RssShuffleEnvironmentConfiguration configuration =
        RssShuffleEnvironmentConfiguration.fromConfiguration(shuffleEnvironmentContext);
    return new RssShuffleEnvironment(configuration);
  }
}
