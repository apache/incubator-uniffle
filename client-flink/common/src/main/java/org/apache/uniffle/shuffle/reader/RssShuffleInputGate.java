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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.shuffle.RssFlinkConfig;
import org.apache.uniffle.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.shuffle.resource.DefaultRssShuffleResource;
import org.apache.uniffle.shuffle.resource.RssShuffleResourceDescriptor;
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

  private final int[] clientIndexMap;
  private final int[] channelIndexMap;
  private final int[] numSubPartitionsHasNotConsumed;

  private long numUnconsumedSubpartitions;

  private int startSubIndex;
  private int endSubIndex;
  private long pendingEndOfDataEvents;

  private String basePath;

  private final Map<Integer, List<ShuffleReadClient>> shuffleReadClientMapping =
      new LinkedHashMap<>();

  private Queue<Pair<Buffer, InputChannelInfo>> receivedBuffers = new LinkedList<>();

  private RssConf rssConf;

  private org.apache.hadoop.conf.Configuration hadoopConfiguration;

  private String clientType;

  protected ShuffleWriteClient shuffleWriteClient;

  private final ShuffleDataDistributionType dataDistributionType;

  public RssShuffleInputGate(
      String owningTaskName,
      int gateIndex,
      InputGateDeploymentDescriptor gateDescriptor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int startSubIndex,
      int endSubIndex,
      Configuration conf,
      RssConf rssConf) {

    this.owningTaskName = owningTaskName;
    this.gateIndex = gateIndex;
    this.gateDescriptor = gateDescriptor;
    this.bufferPoolFactory = checkNotNull(bufferPoolFactory);
    this.bufferDecompressor = bufferDecompressor;

    int numChannels = gateDescriptor.getShuffleDescriptors().length;
    this.clientIndexMap = new int[numChannels];
    this.channelIndexMap = new int[numChannels];
    this.numSubPartitionsHasNotConsumed = new int[numChannels];

    this.numUnconsumedSubpartitions = initShuffleReadClients();
    this.pendingEndOfDataEvents = numUnconsumedSubpartitions;
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
    this.basePath = conf.getString(RssFlinkConfig.RSS_REMOTE_STORAGE_PATH);
    this.rssConf = rssConf;
    this.hadoopConfiguration = getHadoopConfiguration(conf);

    this.shuffleWriteClient = FlinkShuffleUtils.createShuffleClient(conf);
    clientType = conf.get(RssFlinkConfig.RSS_CLIENT_TYPE);
    this.dataDistributionType = getShuffleDataDistributionType(conf);
  }

  private long initShuffleReadClients() {
    checkState(endSubIndex >= startSubIndex);
    int numSubpartitionsPerChannel = endSubIndex - startSubIndex + 1;
    long numUnconsumedSubpartitions = 0;

    List<Pair<Integer, ShuffleDescriptor>> descriptors = genChannelShuffleDescriptorMappingList();
    for (int clientIndex = 0; clientIndex < descriptors.size(); clientIndex++) {
      Pair<Integer, ShuffleDescriptor> descriptor = descriptors.get(clientIndex);
      RssShuffleDescriptor shuffleDescriptor = (RssShuffleDescriptor) descriptor.getRight();
      List<ShuffleReadClient> shuffleReadClients = initShuffleReadClient(shuffleDescriptor);
      shuffleReadClientMapping.put(clientIndex, shuffleReadClients);
      numSubPartitionsHasNotConsumed[descriptor.getLeft()] = numSubpartitionsPerChannel;
      numUnconsumedSubpartitions += numSubpartitionsPerChannel;
      clientIndexMap[descriptor.getLeft()] = clientIndex;
      channelIndexMap[clientIndex] = descriptor.getLeft();
    }
    return numUnconsumedSubpartitions;
  }

  private List<Pair<Integer, ShuffleDescriptor>> genChannelShuffleDescriptorMappingList() {
    ShuffleDescriptor[] shuffleDescriptors = gateDescriptor.getShuffleDescriptors();
    if (ArrayUtils.isEmpty(shuffleDescriptors)) {
      throw new RuntimeException("ShuffleDescriptors not null!");
    }
    List<Pair<Integer, ShuffleDescriptor>> descriptors = new ArrayList<>();
    for (int i = 0; i < shuffleDescriptors.length; i++) {
      Pair<Integer, ShuffleDescriptor> channelShuffleDescriptorMapping =
          Pair.of(i, shuffleDescriptors[i]);
      descriptors.add(channelShuffleDescriptorMapping);
    }
    return descriptors;
  }

  private List<ShuffleReadClient> initShuffleReadClient(RssShuffleDescriptor shuffleDescriptor) {
    DefaultRssShuffleResource shuffleResource = shuffleDescriptor.getShuffleResource();
    RssShuffleResourceDescriptor shuffleResourceDescriptor =
        shuffleResource.getShuffleResourceDescriptor();

    String appId = shuffleDescriptor.getJobId().toString();
    int shuffleId = shuffleResourceDescriptor.getShuffleId();
    int partitionNum = gateDescriptor.getShuffleDescriptors().length;

    int attemptId = shuffleResourceDescriptor.getAttemptId();

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        shuffleResource.getPartitionToServers();

    Roaring64NavigableMap roaring64NavigableMap =
        genPartitionToExpectBlocks(
            appId, shuffleId, partitionToServers, startSubIndex, endSubIndex);

    List<ShuffleReadClient> shuffleReadClients = new ArrayList<>();

    for (int subpartition = startSubIndex; subpartition <= endSubIndex; subpartition++) {
      List<ShuffleServerInfo> shuffleServerInfos = partitionToServers.get(subpartition);
      boolean expectedTaskIdsBitmapFilterEnable = (shuffleServerInfos.size() > 1);
      ShuffleReadClient shuffleReadClient =
          ShuffleClientFactory.getInstance()
              .createShuffleReadClient(
                  ShuffleClientFactory.newReadBuilder()
                      .appId(appId)
                      .shuffleId(shuffleId)
                      .partitionId(subpartition)
                      .basePath(basePath)
                      .partitionNumPerRange(1)
                      .partitionNum(partitionNum)
                      .blockIdBitmap(roaring64NavigableMap)
                      .taskIdBitmap(genTaskIdBitmap(attemptId))
                      .shuffleServerInfoList(shuffleServerInfos)
                      .hadoopConf(this.hadoopConfiguration)
                      .shuffleDataDistributionType(this.dataDistributionType)
                      .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                      .rssConf(rssConf));
      shuffleReadClients.add(shuffleReadClient);
    }

    return shuffleReadClients;
  }

  private Roaring64NavigableMap genTaskIdBitmap(int attemptId) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    taskIdBitmap.addLong(attemptId);
    return taskIdBitmap;
  }

  private Roaring64NavigableMap genPartitionToExpectBlocks(
      String appId,
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> allPartitionToServers,
      int startSubIndex,
      int endSubIndex) {
    Map<Integer, List<ShuffleServerInfo>> requirePartitionToServers =
        allPartitionToServers.entrySet().stream()
            .filter(x -> x.getKey() >= startSubIndex && x.getKey() < endSubIndex)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        RssUtils.generateServerToPartitions(requirePartitionToServers);
    return getShuffleResultForMultiPart(clientType, serverToPartitions, appId, shuffleId);
  }

  private Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId) {
    Set<Integer> failedPartitions = Sets.newHashSet();
    return shuffleWriteClient.getShuffleResultForMultiPart(
        clientType, serverToPartitions, appId, shuffleId, failedPartitions);
  }

  @Override
  public int getGateIndex() {
    return gateIndex;
  }

  @Override
  public List<InputChannelInfo> getUnfinishedChannels() {
    return Collections.emptyList();
  }

  @Override
  public int getBuffersInUseCount() {
    return 0;
  }

  @Override
  public void announceBufferSize(int bufferSize) {}

  @Override
  public int getNumberOfInputChannels() {
    return shuffleReadClientMapping.size();
  }

  @Override
  public boolean isFinished() {
    /*synchronized (lock) {
      return allReadersEOF() && receivedBuffers.isEmpty();
    }*/
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
    List<ShuffleReadClient> allShuffleReadClients = new ArrayList<>();
    for (int i = 0; i < gateDescriptor.getShuffleDescriptors().length; i++) {
      List<ShuffleReadClient> shuffleReadClients = shuffleReadClientMapping.get(i);
      allShuffleReadClients.addAll(shuffleReadClients);
    }
    allShuffleReadClients.stream().parallel().forEach(client -> {
      client.readShuffleBlockData();
    });
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
