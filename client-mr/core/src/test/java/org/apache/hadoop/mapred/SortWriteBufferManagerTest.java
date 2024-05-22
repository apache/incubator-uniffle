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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class SortWriteBufferManagerTest {

  @Test
  public void testWriteException() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager;
    manager =
        new SortWriteBufferManager<BytesWritable, BytesWritable>(
            10240,
            1L,
            10,
            serializationFactory.getSerializer(BytesWritable.class),
            serializationFactory.getSerializer(BytesWritable.class),
            WritableComparator.get(BytesWritable.class),
            0.9,
            "test",
            client,
            500,
            5 * 1000,
            partitionToServers,
            successBlocks,
            failedBlocks,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            1,
            100,
            1000,
            true,
            5,
            0.2f,
            1024000L,
            new RssConf(),
            null);

    // case 1
    Random random = new Random();
    partitionToServers.put(1, Lists.newArrayList(mock(ShuffleServerInfo.class)));
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      manager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }
    RssException rssException = assertThrows(RssException.class, manager::waitSendFinished);
    assertTrue(rssException.getMessage().contains("Timeout"));

    // case 2
    client.setMode(1);
    SortWriteBufferManager<BytesWritable, BytesWritable> finalManager = manager;
    rssException =
        assertThrows(
            RssException.class,
            () -> {
              for (int i = 0; i < 1000; i++) {
                byte[] key = new byte[20];
                byte[] value = new byte[1024];
                random.nextBytes(key);
                random.nextBytes(value);
                finalManager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
              }
            });
    assertFalse(failedBlocks.isEmpty());
    assertTrue(rssException.getMessage().contains("Send failed"));

    rssException = assertThrows(RssException.class, finalManager::waitSendFinished);
    assertTrue(rssException.getMessage().contains("Send failed"));

    // case 3
    client.setMode(0);
    manager =
        new SortWriteBufferManager<BytesWritable, BytesWritable>(
            100,
            1L,
            10,
            serializationFactory.getSerializer(BytesWritable.class),
            serializationFactory.getSerializer(BytesWritable.class),
            WritableComparator.get(BytesWritable.class),
            0.9,
            "test",
            client,
            500,
            5 * 1000,
            partitionToServers,
            Sets.newConcurrentHashSet(),
            Sets.newConcurrentHashSet(),
            mapOutputByteCounter,
            mapOutputRecordCounter,
            1,
            100,
            1000,
            true,
            5,
            0.2f,
            1024000L,
            new RssConf(),
            null);
    byte[] key = new byte[20];
    byte[] value = new byte[1024];
    random.nextBytes(key);
    random.nextBytes(value);

    SortWriteBufferManager<BytesWritable, BytesWritable> finalManager1 = manager;
    rssException =
        assertThrows(
            RssException.class,
            () -> finalManager1.addRecord(1, new BytesWritable(key), new BytesWritable(value)));
    assertTrue(rssException.getMessage().contains("too big"));
  }

  @Test
  public void testOnePartition() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(2);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager;
    manager =
        new SortWriteBufferManager<BytesWritable, BytesWritable>(
            10240,
            1L,
            10,
            serializationFactory.getSerializer(BytesWritable.class),
            serializationFactory.getSerializer(BytesWritable.class),
            WritableComparator.get(BytesWritable.class),
            0.9,
            "test",
            client,
            500,
            5 * 1000,
            partitionToServers,
            successBlocks,
            failedBlocks,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            1,
            100,
            2000,
            true,
            5,
            0.2f,
            100L,
            new RssConf(),
            null);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      partitionToServers.put(partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
      manager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
      assertTrue(manager.getWaitSendBuffers().isEmpty());
    }
  }

  @Test
  public void testWriteNormal() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(2);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager;
    manager =
        new SortWriteBufferManager<BytesWritable, BytesWritable>(
            10240,
            1L,
            10,
            serializationFactory.getSerializer(BytesWritable.class),
            serializationFactory.getSerializer(BytesWritable.class),
            WritableComparator.get(BytesWritable.class),
            0.9,
            "test",
            client,
            500,
            5 * 1000,
            partitionToServers,
            successBlocks,
            failedBlocks,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            1,
            100,
            2000,
            true,
            5,
            0.2f,
            1024000L,
            new RssConf(),
            null);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      partitionToServers.put(partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
      manager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }
    manager.waitSendFinished();
    assertTrue(manager.getWaitSendBuffers().isEmpty());
    for (int i = 0; i < 14; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[i * 100];
      random.nextBytes(key);
      random.nextBytes(value);
      manager.addRecord(i, new BytesWritable(key), new BytesWritable(value));
    }
    assertEquals(4, manager.getWaitSendBuffers().size());
    for (int i = 0; i < 4; i++) {
      int dataLength = manager.getWaitSendBuffers().get(i).getDataLength();
      assertEquals((3 - i) * 100 + 28, dataLength);
    }
    manager.waitSendFinished();
    assertTrue(manager.getWaitSendBuffers().isEmpty());
  }

  @Test
  public void testCommitBlocksWhenMemoryShuffleDisabled() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(3);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager;
    manager =
        new SortWriteBufferManager<BytesWritable, BytesWritable>(
            10240,
            1L,
            10,
            serializationFactory.getSerializer(BytesWritable.class),
            serializationFactory.getSerializer(BytesWritable.class),
            WritableComparator.get(BytesWritable.class),
            0.9,
            "test",
            client,
            500,
            5 * 1000,
            partitionToServers,
            successBlocks,
            failedBlocks,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            1,
            100,
            1,
            false,
            5,
            0.2f,
            1024000L,
            new RssConf(),
            null);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      partitionToServers.put(partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
      manager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }
    manager.waitSendFinished();
    assertTrue(manager.getWaitSendBuffers().isEmpty());
    // When MEMOEY storage type is disable, all blocks should flush.
    assertEquals(
        client.mockedShuffleServer.getFinishBlockSize(),
        client.mockedShuffleServer.getFlushBlockSize());
  }

  @Test
  public void testCombineBuffer() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(IntWritable.class);
    jobConf.setCombinerClass(Reduce.class);
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    Serializer<Text> keySerializer = serializationFactory.getSerializer(Text.class);
    Serializer<IntWritable> valueSerializer = serializationFactory.getSerializer(IntWritable.class);
    WritableComparator comparator = WritableComparator.get(Text.class);

    Task.TaskReporter reporter = mock(Task.TaskReporter.class);

    final Counters.Counter combineInputCounter = new Counters.Counter();

    Task.CombinerRunner<Text, IntWritable> combinerRunner =
        Task.CombinerRunner.create(
            jobConf, new TaskAttemptID(), combineInputCounter, reporter, null);

    SortWriteBuffer<Text, IntWritable> buffer =
        new SortWriteBuffer<Text, IntWritable>(1, comparator, 3072, keySerializer, valueSerializer);

    List<String> wordTable =
        Lists.newArrayList(
            "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");
    Random random = new Random();
    for (int i = 0; i < 8; i++) {
      buffer.addRecord(new Text(wordTable.get(i)), new IntWritable(1));
    }
    for (int i = 0; i < 10000; i++) {
      int index = random.nextInt(wordTable.size());
      buffer.addRecord(new Text(wordTable.get(index)), new IntWritable(1));
    }

    SortWriteBufferManager<Text, IntWritable> manager =
        new SortWriteBufferManager<Text, IntWritable>(
            10240,
            1L,
            10,
            keySerializer,
            valueSerializer,
            comparator,
            0.9,
            "test",
            null,
            500,
            5 * 1000,
            null,
            null,
            null,
            null,
            null,
            1,
            100,
            1,
            true,
            5,
            0.2f,
            1024000L,
            new RssConf(),
            combinerRunner);

    buffer.sort();
    SortWriteBuffer<Text, IntWritable> newBuffer = manager.combineBuffer(buffer);

    RawKeyValueIterator kvIterator1 = new SortWriteBuffer.SortBufferIterator<>(buffer);
    RawKeyValueIterator kvIterator2 = new SortWriteBuffer.SortBufferIterator<>(newBuffer);
    int count1 = 0;
    while (kvIterator1.next()) {
      count1++;
    }
    int count2 = 0;
    while (kvIterator2.next()) {
      count2++;
    }
    assertEquals(10008, count1);
    assertEquals(8, count2);
  }

  class MockShuffleServer {

    // All methods of MockShuffle are thread safe, because send-thread may do something in
    // concurrent way.
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
    public SendShuffleDataResult sendShuffleData(
        String appId,
        List<ShuffleBlockInfo> shuffleBlockInfoList,
        Supplier<Boolean> needCancelRequest) {
      if (mode == 0) {
        throw new RssException("send data failed");
      } else if (mode == 1) {
        FailedBlockSendTracker failedBlockSendTracker = new FailedBlockSendTracker();
        ShuffleBlockInfo failedBlock =
            new ShuffleBlockInfo(1, 1, 3, 1, 1, new byte[1], null, 1, 100, 1);
        failedBlockSendTracker.add(
            failedBlock, new ShuffleServerInfo("host", 39998), StatusCode.NO_BUFFER);
        return new SendShuffleDataResult(Sets.newHashSet(2L), failedBlockSendTracker);
      } else {
        if (mode == 3) {
          try {
            Thread.sleep(10);
            mockedShuffleServer.addCachedBlockInfos(shuffleBlockInfoList);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RssException(e);
          }
        }
        Set<Long> successBlockIds = Sets.newHashSet();
        for (ShuffleBlockInfo blockInfo : shuffleBlockInfoList) {
          successBlockIds.add(blockInfo.getBlockId());
        }
        return new SendShuffleDataResult(successBlockIds, new FailedBlockSendTracker());
      }
    }

    @Override
    public void sendAppHeartbeat(String appId, long timeoutMs) {}

    @Override
    public void registerApplicationInfo(String appId, long timeoutMs, String user) {}

    @Override
    public void registerShuffle(
        ShuffleServerInfo shuffleServerInfo,
        String appId,
        int shuffleId,
        List<PartitionRange> partitionRanges,
        RemoteStorageInfo remoteStorage,
        ShuffleDataDistributionType distributionType,
        int maxConcurrencyPerPartitionToWrite) {}

    @Override
    public boolean sendCommit(
        Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
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
    public void registerCoordinators(String coordinators) {}

    @Override
    public Map<String, String> fetchClientConf(int timeoutMs) {
      return null;
    }

    @Override
    public RemoteStorageInfo fetchRemoteStorage(String appId) {
      return null;
    }

    @Override
    public void reportShuffleResult(
        Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
        String appId,
        int shuffleId,
        long taskAttemptId,
        int bitmapNum) {
      if (mode == 3) {
        serverToPartitionToBlockIds
            .values()
            .forEach(
                partitionToBlockIds -> {
                  mockedShuffleServer.addFinishedBlockInfos(
                      partitionToBlockIds.values().stream()
                          .flatMap(it -> it.stream())
                          .collect(Collectors.toList()));
                });
      }
    }

    @Override
    public ShuffleAssignmentsInfo getShuffleAssignments(
        String appId,
        int shuffleId,
        int partitionNum,
        int partitionNumPerRange,
        Set<String> requiredTags,
        int assignmentShuffleServerNumber,
        int estimateTaskConcurrency) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResult(
        String clientType,
        Set<ShuffleServerInfo> shuffleServerInfoSet,
        String appId,
        int shuffleId,
        int partitionId) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResultForMultiPart(
        String clientType,
        Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
        String appId,
        int shuffleId,
        Set<Integer> failedPartitions,
        PartitionDataReplicaRequirementTracking tracking) {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public void unregisterShuffle(String appId, int shuffleId) {}

    @Override
    public void unregisterShuffle(String appId) {}
  }

  static class Reduce extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, IntWritable> {

    Reduce() {}

    public void reduce(
        Text key,
        Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }
}
