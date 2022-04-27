/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SortWriteBufferManagerTest {

  @Test
  public void testWriteException() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager = new SortWriteBufferManager(
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
        true);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      manager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }
    boolean isException = false;
    try {
      manager.waitSendFinished();
    } catch (RssException re) {
      isException = true;
    }
    assertTrue(isException);
    client.setMode(1);
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      manager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }
    assertFalse(failedBlocks.isEmpty());
    isException = false;
    manager = new SortWriteBufferManager(
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
        successBlocks,
        failedBlocks,
        mapOutputByteCounter,
        mapOutputRecordCounter,
        1,
        100,
        1000,
        true);
    byte[] key = new byte[20];
    byte[] value = new byte[1024];
    random.nextBytes(key);
    random.nextBytes(value);
    try {
      manager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    } catch (RssException re) {
      assertTrue(re.getMessage().contains("too big"));
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void testWriteNormal() throws Exception {
    JobConf jobConf = new JobConf(new Configuration());
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(2);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<BytesWritable, BytesWritable> manager = new SortWriteBufferManager(
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
        true);
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      manager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }
    manager.waitSendFinished();
  }

  class MockShuffleWriteClient implements ShuffleWriteClient {

    int mode = 0;

    public void setMode(int mode) {
      this.mode = mode;
    }

    @Override
    public SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList) {
      if (mode == 0) {
        throw new RssException("send data failed");
      } else if (mode == 1) {
        return new SendShuffleDataResult(Sets.newHashSet(2L), Sets.newHashSet(1L));
      } else {
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
    public void registerShuffle(ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId, List<PartitionRange> partitionRanges) {

    }

    @Override
    public boolean sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
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
    public String fetchRemoteStorage(String appId) {
      return null;
    }

    @Override
    public void reportShuffleResult(Map<Integer, List<ShuffleServerInfo>> partitionToServers, String appId, int shuffleId, long taskAttemptId, Map<Integer, List<Long>> partitionToBlockIds, int bitmapNum) {

    }

    @Override
    public ShuffleAssignmentsInfo getShuffleAssignments(String appId, int shuffleId, int partitionNum, int partitionNumPerRange, Set<String> requiredTags) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int partitionId) {
      return null;
    }

    @Override
    public void close() {

    }
  }
}
