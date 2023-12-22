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

package org.apache.uniffle.shuffle.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.shuffle.RssFlinkConfig;
import org.apache.uniffle.shuffle.utils.FlinkShuffleUtils;

import static org.apache.flink.runtime.util.HadoopUtils.getHadoopConfiguration;
import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.uniffle.shuffle.utils.FlinkShuffleUtils.getShuffleDataDistributionType;

/**
 * RssShuffleInputGate is responsible for fetching data from the ShuffleServer and is an
 * implementation of {@link IndexedInputGate}.
 */
public class RssShuffleInputGate extends IndexedInputGate {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleInputGate.class);

  /** Lock object to guard partition requests and runtime channel updates. */
  private Object requestLock = new Object();

  /** The name of the owning task, for logging purposes. */
  private final String owningTaskName;

  /** Index of the gate of the corresponding computing task. */
  private final int gateIndex;

  /** Deployment descriptor for a single input gate instance. */
  private final InputGateDeploymentDescriptor gateDescriptor;

  /** Buffer pool provider. */
  private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

  /** buffer pools to allocate network memory. */
  private BufferPool bufferPool;

  /** Data decompressor. */
  private final BufferDecompressor bufferDecompressor;

  private final int[] numSubPartitionsHasNotConsumed;

  private long numUnconsumedSubpartitions;

  private int startSubIndex;
  private int endSubIndex;
  private long pendingEndOfDataEvents;

  private List<RssInputChannel> rssInputChannels = new ArrayList<>();
  private final List<InputChannelInfo> channelsInfo = new ArrayList<>();

  private String basePath;

  private Queue<ChannelBuffer> receivedBuffers = new LinkedList<>();

  private RssConf rssConf;

  private org.apache.hadoop.conf.Configuration hadoopConfiguration;

  private String clientType;

  protected ShuffleWriteClient shuffleWriteClient;

  private final ShuffleDataDistributionType dataDistributionType;

  private final ExecutorService executor;

  public RssShuffleInputGate(
      String owningTaskName,
      int gateIndex,
      InputGateDeploymentDescriptor gateDescriptor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int startSubIndex,
      int endSubIndex,
      Configuration conf,
      int numConcurrentReading,
      RssConf rssConf) {

    this.owningTaskName = owningTaskName;
    this.gateIndex = gateIndex;
    this.gateDescriptor = gateDescriptor;
    this.bufferPoolFactory = checkNotNull(bufferPoolFactory);
    this.bufferDecompressor = bufferDecompressor;

    int numChannels = gateDescriptor.getShuffleDescriptors().length;
    this.numSubPartitionsHasNotConsumed = new int[numChannels];

    this.numUnconsumedSubpartitions = initShuffleReadClients();
    this.pendingEndOfDataEvents = numUnconsumedSubpartitions;
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
    this.basePath = conf.getString(RssFlinkConfig.RSS_REMOTE_STORAGE_PATH);
    this.rssConf = rssConf;
    this.hadoopConfiguration = getHadoopConfiguration(conf);

    this.shuffleWriteClient = FlinkShuffleUtils.createShuffleClient(conf);
    this.clientType = conf.get(RssFlinkConfig.RSS_CLIENT_TYPE);
    this.dataDistributionType = getShuffleDataDistributionType(conf);
    this.executor = Executors.newFixedThreadPool(numConcurrentReading);
  }

  private long initShuffleReadClients() {
    checkState(endSubIndex >= startSubIndex);
    int numSubpartitionsPerChannel = endSubIndex - startSubIndex + 1;
    long numUnconsumedSubpartitions = 0;
    List<RssInputChannel> rssInputChannels = genShuffleDescriptorWithChannel();
    for (RssInputChannel channel : rssInputChannels) {
      numSubPartitionsHasNotConsumed[channel.getChannelIndex()] = numSubpartitionsPerChannel;
      numUnconsumedSubpartitions += numSubpartitionsPerChannel;
    }
    return numUnconsumedSubpartitions;
  }

  private List<RssInputChannel> genShuffleDescriptorWithChannel() {
    ShuffleDescriptor[] shuffleDescriptors = gateDescriptor.getShuffleDescriptors();
    if (ArrayUtils.isEmpty(shuffleDescriptors)) {
      throw new RuntimeException("ShuffleDescriptors not null!");
    }
    List<RssInputChannel> descriptors = new ArrayList<>();
    for (int i = 0; i < shuffleDescriptors.length; i++) {
      int channelIndex = 0;
      ShuffleDescriptor shuffleDescriptor = shuffleDescriptors[i];
      InputChannelInfo inputChannelInfo = new InputChannelInfo(gateIndex, channelIndex);
      RssInputChannel channel =
          new RssInputChannel(
              shuffleDescriptor, channelIndex, shuffleWriteClient, clientType, inputChannelInfo);
      channel.getShuffleReadClient(
          this.basePath, this.hadoopConfiguration, this.dataDistributionType, this.rssConf);
      descriptors.add(channel);
      channelsInfo.add(inputChannelInfo);
    }
    return descriptors;
  }

  // DONE
  @Override
  public int getGateIndex() {
    return gateIndex;
  }

  // DONE
  @Override
  public List<InputChannelInfo> getUnfinishedChannels() {
    return Collections.emptyList();
  }

  // DONE
  @Override
  public int getBuffersInUseCount() {
    return 0;
  }

  // DONE
  @Override
  public void announceBufferSize(int bufferSize) {}

  // DONE
  @Override
  public int getNumberOfInputChannels() {
    return channelsInfo.size();
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  private boolean allReadersEOF() {
    return numUnconsumedSubpartitions <= 0;
  }

  @Override
  public boolean hasReceivedEndOfData() {
    return false;
  }

  @Override
  public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
    return Optional.empty();
  }

  @Override
  public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {

    return Optional.empty();
  }

  @Override
  public void sendTaskEvent(TaskEvent event) throws IOException {}

  @Override
  public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {}

  @Override
  public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {}

  @Override
  public InputChannel getChannel(int channelIndex) {
    return null;
  }

  @Override
  public void setup() throws IOException {
    bufferPool = bufferPoolFactory.get();
    for (RssInputChannel rssInputChannel : rssInputChannels) {
      ShuffleReadClient shuffleReadClient =
          rssInputChannel.getShuffleReadClient(
              this.basePath, this.hadoopConfiguration, this.dataDistributionType, this.rssConf);
      InputChannelInfo inputChannelInfo = rssInputChannel.getInputChannelInfo();
      this.executor.submit(new RssFetcher(receivedBuffers, shuffleReadClient, inputChannelInfo));
    }
    availabilityHelper.getUnavailableToResetUnavailable().complete(null);
  }

  @Override
  public void requestPartitions() throws IOException {}

  @Override
  public CompletableFuture<Void> getStateConsumedFuture() {
    return null;
  }

  @Override
  public void finishReadRecoveredState() throws IOException {}

  @Override
  public void close() throws Exception {}
}
