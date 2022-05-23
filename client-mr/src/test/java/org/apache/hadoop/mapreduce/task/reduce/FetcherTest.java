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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.RssShuffleUtils;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MROutputFiles;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SortWriteBufferManager;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.junit.jupiter.api.Test;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class FetcherTest {
  static JobID jobId = new JobID("a", 0);
  static TaskAttemptID reduceId1 = new TaskAttemptID(
    new TaskID(jobId, TaskType.REDUCE, 0), 0);
  static Configuration conf = new Configuration();
  static JobConf jobConf = new JobConf();
  static LocalDirAllocator lda = new LocalDirAllocator(MRConfig.LOCAL_DIR);

  static TaskStatus taskStatus = new MockedTaskStatus();
  static ShuffleClientMetrics metrics = new ShuffleClientMetrics(reduceId1, jobConf);
  static Reporter reporter = new MockedReporter();
  static FileSystem fs;
  static List<byte[]> data;
  static MergeManagerImpl<Text, Text> merger;

  @Test
  public void writeAndReadDataTestWithRss() throws Throwable {
    fs = FileSystem.getLocal(conf);
    initRssData();
    merger = new MergeManagerImpl<Text, Text>(
        reduceId1, jobConf, fs, lda, Reporter.NULL, null, null, null, null, null,
        null, null, new Progress(), new MROutputFiles());
    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(data);
    RssFetcher fetcher = new RssFetcher(jobConf, reduceId1, taskStatus, merger, new Progress(),
      reporter, metrics, shuffleReadClient, 3);
    fetcher.fetchAllRssBlocks();


    RawKeyValueIterator iterator = merger.close();
    // the final output of merger.close() must be sorted
    List<String> allKeysExpected = Lists.newArrayList("k11", "k22", "k22", "k33", "k44", "k55", "k55");
    List<String> allValuesExpected = Lists.newArrayList("v11", "v22", "v22", "v33", "v44", "v55", "v55");
    List<String> allKeys = Lists.newArrayList();
    List<String> allValues = Lists.newArrayList();
    while(iterator.next()){
      byte[] key = new byte[iterator.getKey().getLength()];
      byte[] value = new byte[iterator.getValue().getLength()];
      System.arraycopy(iterator.getKey().getData(), 0, key, 0, key.length);
      System.arraycopy(iterator.getValue().getData(), 0, value, 0, value.length);
      allKeys.add(new Text(key).toString().trim());
      allValues.add(new Text(value).toString().trim());
    }
    validate(allKeysExpected, allKeys);
    validate(allValuesExpected, allValues);
  }

  @Test
  public void writeAndReadDataTestWithoutRss() throws Throwable {
    fs = FileSystem.getLocal(conf);
    initLocalData();
    merger = new MergeManagerImpl<Text, Text>(
        reduceId1, jobConf, fs, lda, Reporter.NULL, null, null, null, null, null,
        null, null, new Progress(), new MROutputFiles());
    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(data);
    RssFetcher fetcher = new RssFetcher(jobConf, reduceId1, taskStatus, merger, new Progress(),
        reporter, metrics, shuffleReadClient, 3);
    fetcher.fetchAllRssBlocks();


    RawKeyValueIterator iterator = merger.close();
    // the final output of merger.close() must be sorted
    List<String> allKeysExpected = Lists.newArrayList("k11", "k22", "k22", "k33", "k44", "k55", "k55");
    List<String> allValuesExpected = Lists.newArrayList("v11", "v22", "v22", "v33", "v44", "v55", "v55");
    List<String> allKeys = Lists.newArrayList();
    List<String> allValues = Lists.newArrayList();
    while(iterator.next()){
      byte[] key = new byte[iterator.getKey().getLength()];
      byte[] value = new byte[iterator.getValue().getLength()];
      System.arraycopy(iterator.getKey().getData(), 0, key, 0, key.length);
      System.arraycopy(iterator.getValue().getData(), 0, value, 0, value.length);
      allKeys.add(new Text(key).toString().trim());
      allValues.add(new Text(value).toString().trim());
    }
    validate(allKeysExpected, allKeys);
    validate(allValuesExpected, allValues);
  }


  private void validate(List<String> expected, List<String> actual) {
    assert(expected.size() == actual.size());
    for(int i = 0; i < expected.size(); i++) {
      assert(expected.get(i).equals(actual.get(i)));
    }
  }

  private static void initRssData() throws Exception {
    data = new LinkedList<>();
    Map<String, String> map1 = new TreeMap<>();
    map1.put("k11", "v11");
    map1.put("k22", "v22");
    map1.put("k44", "v44");
    data.add(writeMapOutputRss(conf, map1));
    Map<String, String> map2 = new TreeMap<>();
    map2.put("k33", "v33");
    map2.put("k55", "v55");
    data.add(writeMapOutputRss(conf, map2));
    Map<String, String> map3 = new TreeMap<>();
    map3.put("k22", "v22");
    map3.put("k55", "v55");
    data.add(writeMapOutputRss(conf, map3));
  }

  private static void initLocalData() throws Exception {
    data = new LinkedList<>();
    Map<String, String> map1 = new TreeMap<>();
    map1.put("k11", "v11");
    map1.put("k22", "v22");
    map1.put("k44", "v44");
    data.add(writeMapOutput(conf, map1));
    Map<String, String> map2 = new TreeMap<>();
    map2.put("k33", "v33");
    map2.put("k55", "v55");
    data.add(writeMapOutput(conf, map2));
    Map<String, String> map3 = new TreeMap<>();
    map3.put("k22", "v22");
    map3.put("k55", "v55");
    data.add(writeMapOutput(conf, map3));
  }

  private static byte[] writeMapOutputRss(Configuration conf, Map<String, String> keysToValues)
    throws IOException, InterruptedException {
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(2);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newConcurrentMap();
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    Counters.Counter mapOutputByteCounter = new Counters.Counter();
    Counters.Counter mapOutputRecordCounter = new Counters.Counter();
    SortWriteBufferManager<Text, Text> manager = new SortWriteBufferManager(
      10240,
      1L,
      10,
      serializationFactory.getSerializer(Text.class),
      serializationFactory.getSerializer(Text.class),
      WritableComparator.get(Text.class),
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

    for (String key : keysToValues.keySet()) {
      String value = keysToValues.get(key);
      manager.addRecord(0, new Text(key), new Text(value));
    }
    manager.waitSendFinished();
    return client.data.get(0);
  }


  private static byte[] writeMapOutput(Configuration conf, Map<String, String> keysToValues)
    throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, fsdos,
      Text.class, Text.class, null, null);
    for (String key : keysToValues.keySet()) {
      String value = keysToValues.get(key);
      writer.append(new Text(key), new Text(value));
    }
    writer.close();
    return baos.toByteArray();
  }


  static class MockShuffleWriteClient implements ShuffleWriteClient {

    int mode = 0;

    public void setMode(int mode) {
      this.mode = mode;
    }

    public List<byte[]> data = new LinkedList<>();
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
        shuffleBlockInfoList.forEach( block -> {
          data.add(RssShuffleUtils.decompressData(block.getData(), block.getUncompressLength()));
          System.out.println("A block is sent");
        });
        return new SendShuffleDataResult(successBlockIds, Sets.newHashSet());
      }
    }

    @Override
    public void sendAppHeartbeat(String appId, long timeoutMs) {

    }

    @Override
    public void registerShuffle(
        ShuffleServerInfo shuffleServerInfo,
        String appId,
        int shuffleId,
        List<PartitionRange> partitionRanges,
        String storageType) {

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

  static class MockedShuffleReadClient implements ShuffleReadClient {
    List<CompressedShuffleBlock> blocks;
    int index = 0;
    MockedShuffleReadClient(List<byte[]> data) {
      this.blocks = new LinkedList<>();
      data.forEach( bytes -> {
        byte[] compressed = RssShuffleUtils.compressData(bytes);
        blocks.add(new CompressedShuffleBlock(ByteBuffer.wrap(compressed), bytes.length));
      });
    }

    @Override
    public CompressedShuffleBlock readShuffleBlockData() {
      if (index < blocks.size()) {
        return blocks.get(index++);
      } else {
        return null;
      }
    }

    @Override
    public void checkProcessedBlockIds() {
    }

    @Override
    public void close() {
    }

    @Override
    public void logStatics() {
    }
  }

  static class MockedReporter implements Reporter {
    MockedReporter() {
    }

    @Override
    public void setStatus(String s) {
    }

    @Override
    public Counters.Counter getCounter(Enum<?> anEnum) {
      return null;
    }

    @Override
    public Counters.Counter getCounter(String s, String s1) {
      return null;
    }

    @Override
    public void incrCounter(Enum<?> anEnum, long l) {
    }

    @Override
    public void incrCounter(String s, String s1, long l) {
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void progress() {
    }
  }

  static class MockedTaskStatus extends TaskStatus {

    @Override
    public boolean getIsMap() {
      return false;
    }

    @Override
    public void addFetchFailedMap(org.apache.hadoop.mapred.TaskAttemptID taskAttemptID) {
    }
  }
}

