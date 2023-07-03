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

package org.apache.tez.runtime.library.common.sort.buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.util.StorageType;



import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WriteBufferManagerTest {
  @Test
  public void testWriteException() throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    long maxMemSize = 10240;
    String appId = "application_1681717153064_3770270";
    long taskAttemptId = 0;
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =  serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer = serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    String storageType = "MEMORY";
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 5;
    int bitmapSplitNum = 1;
    int shuffleId = getShuffleId(tezTaskAttemptID, 1, 2);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(tezTaskAttemptID, maxMemSize, appId,
        taskAttemptId, successBlockIds, failedBlockIds, writeClient,
        comparator, maxSegmentSize, keySerializer,
        valSerializer, maxBufferSize, memoryThreshold,
        sendThreshold, batch, rssConf, partitionToServers,
        numMaps, isMemoryShuffleEnabled(storageType),
        sendCheckInterval, sendCheckTimeout, bitmapSplitNum, shuffleId, true);

    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      bufferManager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }

    boolean isException = false;
    try {
      bufferManager.waitSendFinished();
    } catch (RssException re) {
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void testWriteNormal() throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    long maxMemSize = 10240;
    String appId = "appattempt_1681717153064_3770270_000001";
    long taskAttemptId = 0;
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    writeClient.setMode(2);
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =  serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer = serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    String storageType = "MEMORY";
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 60 * 1000 * 10L;
    int bitmapSplitNum = 1;
    int shuffleId = getShuffleId(tezTaskAttemptID, 1, 2);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(tezTaskAttemptID, maxMemSize, appId,
        taskAttemptId, successBlockIds, failedBlockIds, writeClient,
        comparator, maxSegmentSize, keySerializer,
        valSerializer, maxBufferSize, memoryThreshold,
        sendThreshold, batch, rssConf, partitionToServers,
        numMaps, isMemoryShuffleEnabled(storageType),
        sendCheckInterval, sendCheckTimeout, bitmapSplitNum, shuffleId, true);

    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      bufferManager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }
    bufferManager.waitSendFinished();
    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());

    for (int i = 0; i < 50; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[i * 100];
      random.nextBytes(key);
      random.nextBytes(value);
      bufferManager.addRecord(i, new BytesWritable(key), new BytesWritable(value));
    }
    assert (1 == bufferManager.getWaitSendBuffers().size());
    assert (4928 == bufferManager.getWaitSendBuffers().get(0).getDataLength());

    bufferManager.waitSendFinished();
    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());
  }

  @Test
  public void testCommitBlocksWhenMemoryShuffleDisabled() throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    long maxMemSize = 10240;
    String appId = "application_1681717153064_3770270";
    long taskAttemptId = 0;
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    writeClient.setMode(3);
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =  serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer = serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 60 * 1000 * 10L;
    int bitmapSplitNum = 1;
    int shuffleId = getShuffleId(tezTaskAttemptID, 1, 2);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(tezTaskAttemptID, maxMemSize, appId,
        taskAttemptId, successBlockIds, failedBlockIds, writeClient,
        comparator, maxSegmentSize, keySerializer,
        valSerializer, maxBufferSize, memoryThreshold,
        sendThreshold, batch, rssConf, partitionToServers,
        numMaps, false,
        sendCheckInterval, sendCheckTimeout, bitmapSplitNum, shuffleId, true);

    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      bufferManager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }
    bufferManager.waitSendFinished();

    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());
    assertEquals(writeClient.mockedShuffleServer.getFinishBlockSize(),
        writeClient.mockedShuffleServer.getFlushBlockSize());
  }

  private int getShuffleId(TezTaskAttemptID tezTaskAttemptID, int upVertexId, int downVertexId) {
    TezVertexID tezVertexID = tezTaskAttemptID.getTaskID().getVertexID();
    int shuffleId = RssTezUtils.computeShuffleId(tezVertexID.getDAGId().getId(), upVertexId, downVertexId);
    return shuffleId;
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.withMemory(StorageType.valueOf(storageType));
  }

  class MockShuffleServer {
    private List<ShuffleBlockInfo> cachedBlockInfos = new ArrayList<>();
    private List<ShuffleBlockInfo> flushBlockInfos = new ArrayList<>();
    private List<Long> finishedBlockInfos = new ArrayList<>();

    public synchronized void finishShuffle() {
      flushBlockInfos.addAll(cachedBlockInfos);
    }

    public synchronized void addCachedBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfoList) {
      cachedBlockInfos.addAll(shuffleBlockInfoList);
    }

    public synchronized void addFinishedBlockInfos(List<Long> shuffleBlockInfoList) {
      finishedBlockInfos.addAll(shuffleBlockInfoList);
    }

    public synchronized int getFlushBlockSize() {
      return flushBlockInfos.size();
    }

    public synchronized int getFinishBlockSize() {
      return finishedBlockInfos.size();
    }
  }

  class MockShuffleWriteClient implements ShuffleWriteClient {

    int mode = 0;
    MockShuffleServer mockedShuffleServer = new MockShuffleServer();
    int committedMaps = 0;

    public void setMode(int mode) {
      this.mode = mode;
    }

    @Override
    public SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList,
                                                 Supplier<Boolean> needCancelRequest) {
      if (mode == 0) {
        throw new RssException("send data failed.");
      } else if (mode == 1) {
        return new SendShuffleDataResult(Sets.newHashSet(2L), Sets.newHashSet(1L));
      } else {
        if (mode == 3) {
          try {
            Thread.sleep(10);
            mockedShuffleServer.addCachedBlockInfos(shuffleBlockInfoList);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RssException(e.toString());
          }
        }
        Set<Long> successBlockIds = Sets.newHashSet();
        for (ShuffleBlockInfo blockInfo : shuffleBlockInfoList) {
          successBlockIds.add(blockInfo.getBlockId());
        }
        return new SendShuffleDataResult(successBlockIds, Sets.newHashSet());
      }
    }

    @Override
    public void sendAppHeartbeat(String appId, long timeoutMs) {

    }

    @Override
    public void registerApplicationInfo(String appId, long timeoutMs, String user) {

    }

    @Override
    public void registerShuffle(ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId,
                                List<PartitionRange> partitionRanges, RemoteStorageInfo remoteStorage,
                                ShuffleDataDistributionType dataDistributionType,
                                int maxConcurrencyPerPartitionToWrite) {

    }


    @Override
    public boolean sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
      if (mode == 3) {
        committedMaps++;
        if (committedMaps >= numMaps) {
          mockedShuffleServer.finishShuffle();
        }
        return true;
      }
      return false;
    }

    @Override
    public void registerCoordinators(String coordinators) {

    }

    @Override
    public Map<String, String> fetchClientConf(int timeoutMs) {
      return null;
    }

    @Override
    public RemoteStorageInfo fetchRemoteStorage(String appId) {
      return null;
    }

    @Override
    public void reportShuffleResult(Map<Integer, List<ShuffleServerInfo>> partitionToServers,
                                    String appId, int shuffleId, long taskAttemptId,
                                    Map<Integer, List<Long>> partitionToBlockIds, int bitmapNum) {
      if (mode == 3) {
        mockedShuffleServer.addFinishedBlockInfos(
            partitionToBlockIds.values().stream().flatMap(it -> it.stream()).collect(Collectors.toList())
        );
      }
    }

    @Override
    public ShuffleAssignmentsInfo getShuffleAssignments(String appId, int shuffleId,
        int partitionNum, int partitionNumPerRange,
        Set<String> requiredTags, int assignmentShuffleServerNumber, int estimateTaskConcurrency) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
        String appId, int shuffleId, int partitionId) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResultForMultiPart(String clientType, Map<ShuffleServerInfo,
        Set<Integer>> serverToPartitions, String appId, int shuffleId) {
      return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void unregisterShuffle(String appId, int shuffleId) {

    }
  }

}
