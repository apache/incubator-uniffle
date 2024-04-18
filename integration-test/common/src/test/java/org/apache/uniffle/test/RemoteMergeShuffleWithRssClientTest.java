package org.apache.uniffle.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.io.IntWritable;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.shuffle.reader.KeyValueReader;
import org.apache.uniffle.client.shuffle.reader.RMRecordsReader;
import org.apache.uniffle.client.shuffle.writer.SumByKeyCombiner;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RemoteMergeShuffleWithRssClientTest extends ShuffleReadWriteBase {

  private final static int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;

  private static ShuffleServerInfo shuffleServerInfo1;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(ServerType.GRPC);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    shuffleServerConf.set(ShuffleServerConf.SERVER_DEFAULT_MERGED_BLOCK_SIZE, "1k");
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 10000000);
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_INITIAL_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    createShuffleServer(shuffleServerConf);
    startServers();
    shuffleServerInfo1 =
        new ShuffleServerInfo(
            "127.0.0.1-20001", grpcShuffleServers.get(0).getIp(), SHUFFLE_SERVER_INITIAL_PORT);
  }

  @BeforeEach
  public void createClient() {
    shuffleWriteClientImpl = new ShuffleWriteClientImpl(
        ShuffleClientFactory.newWriteBuilder().clientType(ClientType.GRPC.name()).retryMax(3).retryIntervalMax(1000)
            .heartBeatThreadNum(1).replica(1).replicaWrite(1).replicaRead(1).replicaSkipEnabled(true)
            .dataTransferPoolSize(1).dataCommitPoolSize(1).unregisterThreadPoolSize(10).unregisterRequestTimeSec(10));
  }

  @AfterEach
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  @Timeout(10)
  public void remoteMergeWriteReadTest(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();

    // 2 register shuffle
    String testAppId = "remoteMergeWriteReadTest" + keyClassName;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1,
        keyClass.getName(),
        valueClass.getName(),
        comparator.getClass().getName(), -1);

    // 3 report shuffle result
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 0, 5, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 2, 5, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 4, 5, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 1, 5, 1009, 1));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 3, 5, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo1));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = ImmutableMap.of(PARTITION_ID, new HashSet());
    ptb.get(PARTITION_ID).addAll(blocks1.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    ptb.get(PARTITION_ID).addAll(blocks2.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, ptb);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
    ptb.get(PARTITION_ID).stream().forEach(block -> uniqueBlockIds.add(block));
    shuffleWriteClientImpl.reportUniqueBlocks(Sets.newHashSet(shuffleServerInfo1), testAppId, SHUFFLE_ID, PARTITION_ID,
        uniqueBlockIds);

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo1));
    RMRecordsReader reader = new RMRecordsReader(testAppId, SHUFFLE_ID, Sets.newHashSet(PARTITION_ID), serverInfoMap,
        rssConf, keyClass, valueClass, comparator, null, false, null);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(5 * 1009, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  @Timeout(10)
  public void remoteMergeWriteReadTestWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();
    final SumByKeyCombiner combiner = new SumByKeyCombiner();

    // 2 register shuffle
    String testAppId = "remoteMergeWriteReadTestWithCombine" + keyClassName;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1,
        keyClass.getName(),
        valueClass.getName(),
        comparator.getClass().getName(),
        -1);
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // 3 report shuffle result
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 0, 3, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 1, 3, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 2, 3, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 0, 3, 1009, 1));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        keyClass, valueClass, 2, 3, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo1));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = ImmutableMap.of(PARTITION_ID, new HashSet());
    ptb.get(PARTITION_ID).addAll(blocks1.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    ptb.get(PARTITION_ID).addAll(blocks2.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, ptb);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
    ptb.get(PARTITION_ID).stream().forEach(block -> uniqueBlockIds.add(block));
    shuffleWriteClientImpl.reportUniqueBlocks(Sets.newHashSet(shuffleServerInfo1), testAppId, SHUFFLE_ID, PARTITION_ID,
        uniqueBlockIds);

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo1));
    RMRecordsReader reader = new RMRecordsReader(testAppId, SHUFFLE_ID, Sets.newHashSet(PARTITION_ID), serverInfoMap,
        rssConf, keyClass, valueClass, comparator, combiner, false, null);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      Object value = SerializerUtils.genData(valueClass, index);
      Object newValue = value;
      if (index % 3 != 1) {
        if (value instanceof IntWritable) {
          newValue = new IntWritable(((IntWritable) value).get() * 2);
        } else {
          newValue = (int) value * 2;
        }
      }
      assertEquals(newValue, keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(3 * 1009, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  @Timeout(10)
  public void remoteMergeWriteReadTestMultiPartition(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();

    // 2 register shuffle
    String testAppId = "remoteMergeWriteReadTestMultiPartition" + keyClassName;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(PARTITION_ID, PARTITION_ID),
            new PartitionRange(PARTITION_ID + 1, PARTITION_ID + 1),
            new PartitionRange(PARTITION_ID + 2, PARTITION_ID + 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1,
        keyClass.getName(),
        valueClass.getName(),
        comparator.getClass().getName(),
        -1);

    // 3 report shuffle result
    // this shuffle have three partition, which is hash by key index mode 3
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 0, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 0, 6, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 2, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 2, 6, 1009, 1));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 1, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 4, 6, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 1,  Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 1, 6, 1009, 1));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 0, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 3, 6, 1009, 1));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 2, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 5, 6, 1009, 1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = ImmutableMap.of(
        PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 1, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 2, Lists.newArrayList(shuffleServerInfo1));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = new HashMap<>();
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      final int partitionId = i;
      ptb.put(partitionId, new HashSet());
      ptb.get(partitionId).addAll(blocks1.stream().filter(s -> s.getPartitionId() == partitionId).
          map(s -> s.getBlockId()).collect(Collectors.toList()));
      ptb.get(partitionId).addAll(blocks2.stream().filter(s -> s.getPartitionId() == partitionId).
          map(s -> s.getBlockId()).collect(Collectors.toList()));
    }
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, ptb);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
      ptb.get(i).stream().forEach(block -> uniqueBlockIds.add(block));
      shuffleWriteClientImpl.reportUniqueBlocks(Sets.newHashSet(shuffleServerInfo1), testAppId, SHUFFLE_ID, i,
          uniqueBlockIds);
    }

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap = ImmutableMap.of(
        PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 1, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 2, Lists.newArrayList(shuffleServerInfo1));
    RMRecordsReader reader = new RMRecordsReader(testAppId, SHUFFLE_ID,
        Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2), serverInfoMap, rssConf, keyClass, valueClass,
        comparator, null, false, null);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(6 * 1009, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  @Timeout(10)
  public void remoteMergeWriteReadTestMultiPartitionWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();
    final SumByKeyCombiner combiner = new SumByKeyCombiner();

    // 2 register shuffle
    String testAppId = "remoteMergeWriteReadTestMultiPartitionWithCombine" + keyClassName;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo1,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(PARTITION_ID, PARTITION_ID),
            new PartitionRange(PARTITION_ID + 1, PARTITION_ID + 1),
            new PartitionRange(PARTITION_ID + 2, PARTITION_ID + 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1,
        keyClass.getName(),
        valueClass.getName(),
        comparator.getClass().getName(),
        -1);

    // 3 report shuffle result
    // this shuffle have three partition, which is hash by key index mode 3
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 0, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 0, 6, 1009, 2));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 2, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 2, 6, 1009, 2));
    blocks1.add(createShuffleBlockForRemoteMerge(rssConf, layout, 0, 1, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 4, 6, 1009, 2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 1,  Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 1, 6, 1009, 2));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 0, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 3, 6, 1009, 2));
    blocks2.add(createShuffleBlockForRemoteMerge(rssConf, layout, 1, 2, Lists.newArrayList(shuffleServerInfo1), keyClass,
        valueClass, 5, 6, 1009, 2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = ImmutableMap.of(
        PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 1, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 2, Lists.newArrayList(shuffleServerInfo1));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = new HashMap<>();
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      final int partitionId = i;
      ptb.put(partitionId, new HashSet());
      ptb.get(partitionId).addAll(blocks1.stream().filter(s -> s.getPartitionId() == partitionId).
          map(s -> s.getBlockId()).collect(Collectors.toList()));
      ptb.get(partitionId).addAll(blocks2.stream().filter(s -> s.getPartitionId() == partitionId).
          map(s -> s.getBlockId()).collect(Collectors.toList()));
    }
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo1, ptb);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
      ptb.get(i).stream().forEach(block -> uniqueBlockIds.add(block));
      shuffleWriteClientImpl.reportUniqueBlocks(new HashSet<>(partitionToServers.get(i)), testAppId, SHUFFLE_ID, i,
          uniqueBlockIds);
    }

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap = ImmutableMap.of(
        PARTITION_ID, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 1, Lists.newArrayList(shuffleServerInfo1),
        PARTITION_ID + 2, Lists.newArrayList(shuffleServerInfo1));
    RMRecordsReader reader = new RMRecordsReader(testAppId, SHUFFLE_ID,
        Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2), serverInfoMap, rssConf, keyClass, valueClass,
        comparator, combiner, false, null);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index * 2), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(6 * 1009, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  private final static AtomicInteger ATOMIC_INT_SORTED = new AtomicInteger(0);

  public static ShuffleBlockInfo createShuffleBlockForRemoteMerge(
      RssConf rssConf,
      BlockIdLayout blockIdLayout,
      long taskAttemptId,
      int partitionId,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Class keyClass,
      Class valueClass,
      int start,
      int interval,
      int samples,
      int duplicated) throws IOException {
    long blockId = blockIdLayout.getBlockId(ATOMIC_INT_SORTED.getAndIncrement(), PARTITION_ID, taskAttemptId);
    byte[] buf = SerializerUtils.genSortedRecordBytes(rssConf, keyClass, valueClass, start, interval, samples,
        duplicated);
    return new ShuffleBlockInfo(
        SHUFFLE_ID,
        partitionId,
        blockId,
        buf.length,
        ChecksumUtils.getCrc32(buf),
        buf,
        shuffleServerInfoList,
        buf.length,
        0,
        taskAttemptId);
  }
}
