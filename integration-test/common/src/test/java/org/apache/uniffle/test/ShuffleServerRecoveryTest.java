package org.apache.uniffle.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.segment.LocalOrderSegmentSplitter;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.StatefulUpgradeManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.ShuffleDataDistributionType.LOCAL_ORDER;
import static org.apache.uniffle.test.ShuffleServerWithLocalOfLocalOrderTest.createTestDataWithMultiMapIdx;
import static org.apache.uniffle.test.ShuffleServerWithLocalOfLocalOrderTest.validate;

public class ShuffleServerRecoveryTest extends ShuffleReadWriteBase {
  private static File tmpDir = Files.createTempDir();
  private static File dataDir;
  private static String stateLocation;
  static {
    dataDir = new File(tmpDir, "data1");
    stateLocation = tmpDir.getAbsolutePath() + "/state.bin";
  }

  private static void setupCoordinator() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    coordinators.get(0).start();
  }

  private static void setupServers(boolean recoverableStart) throws Exception {
    shuffleServers = new ArrayList<>();
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    shuffleServerConf.setString("rss.server.app.expired.withoutHeartbeat", "5000");
    shuffleServerConf.setBoolean("rss.server.stateful.upgrade.enable", true);
    shuffleServerConf.setString("rss.server.stateful.upgrade.state.export.location", stateLocation);
    shuffleServers.add(new ShuffleServer(shuffleServerConf, recoverableStart));
    shuffleServers.get(0).start();
  }

  @Test
  public void testWriteAndReadInRecovery() throws Exception {
    setupCoordinator();
    setupServers(false);

    ShuffleServerGrpcClient shuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);

    String testAppId = "testWriteAndReadInRecovery";

    for (int i = 0; i < 4; i++) {
      RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, 0,
          Lists.newArrayList(new PartitionRange(i, i)), new RemoteStorageInfo(""), "", LOCAL_ORDER);
      shuffleServerClient.registerShuffle(rrsr);
    }

    /**
     * Write the data to shuffle-servers
     */
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap[] bitMaps = new Roaring64NavigableMap[4];

    // Create the shuffle block with the mapIdx
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> partitionToBlocksWithMapIdx =
        createTestDataWithMultiMapIdx(bitMaps, expectedData);

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = partitionToBlocksWithMapIdx.entrySet()
        .stream()
        .map(x ->
            Pair.of(x.getKey(), x.getValue().values().stream().flatMap(a -> a.stream()).collect(Collectors.toList()))
        )
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
        testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);

    /**
     * Restart the shuffle-server from the state
     */
    ShuffleServer shuffleServer = shuffleServers.get(0);
    StatefulUpgradeManager statefulUpgradeManager = shuffleServer.getStatefulUpgradeManager();
    statefulUpgradeManager.finalizeAndMaterializeState();
    shuffleServer.stopServer();

    setupServers(true);
    shuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);

    /**
     * Read the data to validate after recoverable start of shuffle-server
     */
    final Set<Long> expectedBlockIds4 = partitionToBlocks.get(0).stream()
        .map(x -> x.getBlockId())
        .collect(Collectors.toSet());
    final Map<Long, byte[]> expectedData4 = expectedData.entrySet().stream()
        .filter(x -> expectedBlockIds4.contains(x.getKey()))
        .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    Roaring64NavigableMap taskIds = Roaring64NavigableMap.bitmapOf();
    for (long blockId : expectedBlockIds4) {
      taskIds.add(new DefaultIdHelper().getTaskAttemptId(blockId));
    }
    ShuffleDataResult sdr  = readShuffleData(
        shuffleServerClient,
        testAppId,
        0,
        0,
        1,
        10,
        10000,
        0,
        new LocalOrderSegmentSplitter(taskIds, 100000)
    );
    validate(
        sdr,
        expectedBlockIds4,
        expectedData4,
        new HashSet<>(Arrays.asList(0L, 1L, 2L))
    );
  }
}
