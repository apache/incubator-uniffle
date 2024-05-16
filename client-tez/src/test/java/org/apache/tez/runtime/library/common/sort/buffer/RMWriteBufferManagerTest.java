package org.apache.tez.runtime.library.common.sort.buffer;

import static org.apache.tez.common.RssTezConfig.RSS_MERGED_WRITE_MAX_RECORDS_DEFAULT;
import static org.apache.tez.common.RssTezConfig.RSS_MERGED_WRITE_MAX_RECORDS_PER_BUFFER_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
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
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RMWriteBufferManagerTest {

  private final static int RECORDS = 1009;

  @Test
  public void testWriteNormal() throws Exception {
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(3);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    partitionToServers.put(0, new ArrayList());
    partitionToServers.get(0).add(new ShuffleServerInfo("host", 39998));
    Set<Long> successBlocks = Sets.newConcurrentHashSet();
    Set<Long> failedBlocks = Sets.newConcurrentHashSet();
    RMWriteBufferManager<Text, IntWritable> manager;
    RssConf rssConf = new RssConf();
    TezCounters counter = new TezCounters();
    TezCounter mapOutputByteCounter = counter.findCounter("group", "bytes");
    TezCounter mapOutputRecordCounter = counter.findCounter("group", "records");
    double sendThreshold = 0.2f;
    long maxBufferSize = 14 * 1024;
    manager =
        new RMWriteBufferManager<Text, IntWritable>(
            10240,
            1L,
            0,
            10,
            new Text.Comparator(),
            0.8,
            "app1",
            client,
            500,
            5 * 1000,
            partitionToServers,
            successBlocks,
            failedBlocks,
            1,
            2000,
            true,
            5,
            sendThreshold,
            rssConf,
            Text.class,
            IntWritable.class,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            maxBufferSize,
            RSS_MERGED_WRITE_MAX_RECORDS_PER_BUFFER_DEFAULT,
            RSS_MERGED_WRITE_MAX_RECORDS_DEFAULT);
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      manager.addRecord(0, (Text) SerializerUtils.genData(Text.class, index),
          (IntWritable) SerializerUtils.genData(IntWritable.class, index));
    }
    manager.waitSendFinished();
    assertEquals(RECORDS, mapOutputRecordCounter.getValue());
    assertEquals(0, manager.getInSendListBytes());
    assertEquals(0, manager.getNumRecordInBuffer());

    // check blocks
    List<ShuffleBlockInfo> blockInfos = client.getCachedBlockInfos();
    assertEquals(1, blockInfos.size());
    ByteBuf buf = blockInfos.get(0).getData();
    byte[] bytes = new byte[blockInfos.get(0).getLength()];
    buf.readBytes(bytes);
    RecordsReader<Text, IntWritable> reader = new RecordsReader<>(rssConf,
        PartialInputStreamImpl.newInputStream(bytes, 0, bytes.length), Text.class, IntWritable.class);
    int index = 0;
    while (reader.hasNext()) {
      reader.next();
      assertEquals(SerializerUtils.genData(Text.class, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(IntWritable.class, index), reader.getCurrentValue());
      index++;
    }
    reader.close();
    assertEquals(RECORDS, index);
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

    public List<ShuffleBlockInfo> getCachedBlockInfos() {
      return cachedBlockInfos;
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
        int maxConcurrencyPerPartitionToWrite,
        String keyClassName,
        String valueClassName,
        String comparatorClassName,
        int mergedBlockSize,
        String mergeClassLoader) {}

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
    public void reportShuffleResult(Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
                                    String appId, int shuffleId, long taskAttemptId, int bitmapNum) {
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
    public void reportUniqueBlocks(Set<ShuffleServerInfo> serverInfos, String appId, int shuffleId, int partitionId,
                                   Roaring64NavigableMap expectedTaskIds) {}

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
        Set<Integer> failedPartitions) {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public void unregisterShuffle(String appId, int shuffleId) {}

    @Override
    public void unregisterShuffle(String appId) {}

    public List<ShuffleBlockInfo> getCachedBlockInfos() {
      return mockedShuffleServer.getCachedBlockInfos();
    }
  }

}
