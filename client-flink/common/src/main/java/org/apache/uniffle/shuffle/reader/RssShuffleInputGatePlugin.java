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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.shuffle.RssFlinkConfig;
import org.apache.uniffle.shuffle.reader.fake.FakedRssInputChannel;
import org.apache.uniffle.shuffle.utils.ShuffleUtils;

import static org.apache.flink.runtime.util.HadoopUtils.getHadoopConfiguration;
import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.uniffle.shuffle.utils.ShuffleUtils.getShuffleDataDistributionType;

/**
 * RssShuffleInputGate is responsible for fetching data from the ShuffleServer and is an
 * implementation of {@link IndexedInputGate}.
 */
public class RssShuffleInputGatePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleInputGatePlugin.class);

  /** Lock object to guard partition requests and runtime channel updates. */
  private final Object requestLock = new Object();

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

  private final AvailabilityProvider.AvailabilityHelper availabilityHelper;

  private final int[] numSubPartitionsHasNotConsumed;

  private long numUnconsumedSubpartitions;

  private final int startSubIndex;
  private final int endSubIndex;
  private long pendingEndOfDataEvents;

  private final List<RssInputChannel> rssInputChannels = new ArrayList<>();
  private final List<InputChannelInfo> channelsInfo = new ArrayList<>();

  private final String basePath;

  private final Queue<ChannelBuffer> receivedBuffers = new LinkedList<>();

  private final RssConf rssConf;

  private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

  private final String clientType;

  protected ShuffleWriteClient shuffleWriteClient;

  private final ShuffleDataDistributionType dataDistributionType;

  private final ExecutorService executor;

  public RssShuffleInputGatePlugin(
      String owningTaskName,
      int gateIndex,
      AvailabilityProvider.AvailabilityHelper availabilityHelper,
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
    this.availabilityHelper = availabilityHelper;

    int numChannels = gateDescriptor.getShuffleDescriptors().length;
    this.numSubPartitionsHasNotConsumed = new int[numChannels];

    this.numUnconsumedSubpartitions = initShuffleReadClients();
    this.pendingEndOfDataEvents = numUnconsumedSubpartitions;
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
    this.basePath = conf.getString(RssFlinkConfig.RSS_REMOTE_STORAGE_PATH);
    this.rssConf = rssConf;
    this.hadoopConfiguration = getHadoopConfiguration(conf);

    this.shuffleWriteClient = ShuffleUtils.createShuffleClient(conf);

    this.clientType = conf.getString(RssFlinkConfig.RSS_CLIENT_TYPE);
    this.dataDistributionType = getShuffleDataDistributionType(conf);
    this.executor = Executors.newFixedThreadPool(numConcurrentReading);
  }

  // ------------------------------------------------------------------------
  // Setup
  // ------------------------------------------------------------------------

  public void setup() throws IOException {
    BufferPool bufferPool = bufferPoolFactory.get();
    setBufferPool(bufferPool);
    setupChannels();
  }

  public void setBufferPool(BufferPool bufferPool) {
    Preconditions.checkState(
        this.bufferPool == null,
        "Bug in input gate setup logic: buffer pool has" + "already been set for this input gate.");

    this.bufferPool = checkNotNull(bufferPool);
  }

  public void setupChannels() throws IOException {
    // First allocate a single floating buffer to avoid potential deadlock when the exclusive
    // buffer is 0. See FLINK-24035 for more information.
    bufferPool.reserveSegments(1);

    synchronized (requestLock) {
      for (RssInputChannel rssInputChannel : rssInputChannels) {
        ShuffleReadClient shuffleReadClient =
            rssInputChannel.getShuffleReadClient(
                this.basePath, this.hadoopConfiguration, this.dataDistributionType, this.rssConf);
        InputChannelInfo inputChannelInfo = rssInputChannel.getInputChannelInfo();
        this.executor.submit(new RssFetcher(receivedBuffers, shuffleReadClient, inputChannelInfo));
      }
    }
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
    for (int channelIndex = 0; channelIndex < shuffleDescriptors.length; channelIndex++) {
      ShuffleDescriptor shuffleDescriptor = shuffleDescriptors[channelIndex];
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
  public int getGateIndex() {
    return gateIndex;
  }

  // DONE
  public int getNumberOfInputChannels() {
    return channelsInfo.size();
  }

  public boolean isFinished() {
    synchronized (requestLock) {
      return allSubpartitionsConsumed() && receivedBuffers.isEmpty();
    }
  }

  private boolean allSubpartitionsConsumed() {
    return numUnconsumedSubpartitions <= 0;
  }

  public boolean hasReceivedEndOfData() {
    return pendingEndOfDataEvents <= 0;
  }

  // ------------------------------------------------------------------------
  // Consume
  // ------------------------------------------------------------------------

  public Optional<BufferOrEvent> pollNext() throws IOException {
    ChannelBuffer channelBuffer = getReceivedChannelBuffer();
    Optional<BufferOrEvent> bufferOrEvent = Optional.empty();
    while (channelBuffer != null) {
      Buffer buffer = channelBuffer.getBuffer();
      InputChannelInfo channelInfo = channelBuffer.getInputChannelInfo();
      if (buffer.isBuffer()) {
        bufferOrEvent = transformBuffer(buffer, channelInfo);
      } else {
        bufferOrEvent = transformEvent(buffer, channelInfo);
      }

      if (bufferOrEvent.isPresent()) {
        break;
      }
      channelBuffer = getReceivedChannelBuffer();
    }
    return bufferOrEvent;
  }

  private ChannelBuffer getReceivedChannelBuffer() {
    synchronized (requestLock) {
      if (!receivedBuffers.isEmpty()) {
        return receivedBuffers.poll();
      } else {
        if (!allSubpartitionsConsumed()) {
          availabilityHelper.resetUnavailable();
        }
        return null;
      }
    }
  }

  private Optional<BufferOrEvent> transformBuffer(Buffer buf, InputChannelInfo info) {
    return Optional.of(new BufferOrEvent(buf, info, !isFinished(), false));
  }

  private Optional<BufferOrEvent> transformEvent(Buffer buffer, InputChannelInfo channelInfo)
      throws IOException {

    final AbstractEvent event;
    try {
      event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
    } catch (Throwable t) {
      throw new IOException("parse buffer failure.", t);
    } finally {
      buffer.recycleBuffer();
    }

    if (event.getClass() == EndOfPartitionEvent.class) {
      return handleEndOfPartitionEvent(buffer, channelInfo, event);
    } else if (event.getClass() == EndOfData.class) {
      return handleEndOfDataEvent(buffer, channelInfo, event);
    }

    return Optional.of(
        new BufferOrEvent(
            event,
            buffer.getDataType().hasPriority(),
            channelInfo,
            !isFinished(),
            buffer.getSize(),
            false));
  }

  private Optional<BufferOrEvent> handleEndOfPartitionEvent(
      Buffer buffer, InputChannelInfo channelInfo, AbstractEvent event) {
    synchronized (requestLock) {
      int inputChannelIdx = channelInfo.getInputChannelIdx();
      int notConsumedSubPartitionNum = numSubPartitionsHasNotConsumed[inputChannelIdx];
      // If duplicates are found, an exception will be thrown directly.
      if (notConsumedSubPartitionNum == 0) {
        throw new IllegalStateException(
            String.format("Channel %s cannot be closed repeatedly!", channelInfo));
      }

      // A subPartition has been consumed,
      // and we reduce the counter of the corresponding channel.
      numSubPartitionsHasNotConsumed[channelInfo.getInputChannelIdx()]--;
      numUnconsumedSubpartitions--;

      // If the channel has subpartitions that have not been consumed, empty is returned.
      if (numSubPartitionsHasNotConsumed[channelInfo.getInputChannelIdx()] != 0) {
        return Optional.empty();
      }

      // If all subpartitions of the channel are consumed, we will close the channel.
      rssInputChannels.get(inputChannelIdx).close();
      if (allSubpartitionsConsumed()) {
        availabilityHelper.getUnavailableToResetAvailable().complete(null);
      }

      return Optional.of(
          new BufferOrEvent(
              event,
              buffer.getDataType().hasPriority(),
              channelInfo,
              !isFinished(),
              buffer.getSize(),
              false));
    }
  }

  private Optional<BufferOrEvent> handleEndOfDataEvent(
      Buffer buffer, InputChannelInfo channelInfo, AbstractEvent event) {
    synchronized (requestLock) {
      if (pendingEndOfDataEvents <= 0) {
        throw new IllegalStateException("Abnormal number of EndOfData events (too many).");
      }
      --pendingEndOfDataEvents;
    }

    return Optional.of(
        new BufferOrEvent(
            event,
            buffer.getDataType().hasPriority(),
            channelInfo,
            !isFinished(),
            buffer.getSize(),
            false));
  }

  public void close() throws Exception {
    synchronized (requestLock) {
      for (RssInputChannel rssInputChannel : rssInputChannels) {
        if (rssInputChannel != null) {
          rssInputChannel.close();
        }
      }
      List<Buffer> buffersToRecycle =
          receivedBuffers.stream().map(ChannelBuffer::getBuffer).collect(Collectors.toList());
      buffersToRecycle.forEach(Buffer::recycleBuffer);
      receivedBuffers.clear();
      if (this.bufferPool != null) {
        this.bufferPool.lazyDestroy();
      }
    }
  }

  @Override
  public String toString() {
    return "RssShuffleInputGate{"
        + "owningTaskName='"
        + owningTaskName
        + '\''
        + ", gateIndex="
        + gateIndex
        + ", gateDescriptor="
        + gateDescriptor.toString()
        + '}';
  }

  // ------------------------------------------------------------------------
  // Fake InputChannel.
  // ------------------------------------------------------------------------

  public InputChannel getChannel(int channelIndex) {
    return new FakedRssInputChannel(this.gateIndex, channelIndex);
  }

}
