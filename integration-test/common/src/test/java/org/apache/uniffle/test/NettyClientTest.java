package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NettyClientTest {
  private AtomicLong ATOMIC_LONG = new AtomicLong();
  @Test
  public void test() throws Exception {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
    File dataFolder = Files.createTempDirectory("rssdata").toFile();
    shuffleServerConf.set(ShuffleServerConf.NETTY_SERVER_ENABLED, true);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, "MEMORY_LOCALFILE_HDFS");
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Collections.singletonList(dataFolder.getAbsolutePath()));
    shuffleServerConf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "fake.coordinator:123");
    ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();

    ShuffleServerGrpcNettyClient client = new ShuffleServerGrpcNettyClient("127.0.0.1", 19999, 29999);

    int shuffleId = 0;
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleBlocks = Maps.newHashMap();
    Map<Integer, List<ShuffleBlockInfo>> part1 = Maps.newHashMap();
    List<ShuffleBlockInfo> part1Blocks = Lists.newArrayList();
    generateTestData(shuffleId, 1, 10, 100, 0, part1Blocks);
    part1.put(1, part1Blocks);

    List<ShuffleBlockInfo> part2Blocks = Lists.newArrayList();
    generateTestData(shuffleId, 2, 20, 100, 0, part1Blocks);
    part1.put(2, part2Blocks);

    shuffleBlocks.put(shuffleId, part1);

    String appId = "test_app";
    RssSendShuffleDataRequest request = new RssSendShuffleDataRequest(appId, 1, 10, shuffleBlocks);
    RssRegisterShuffleRequest registerShuffleRequest = new RssRegisterShuffleRequest(appId, shuffleId, Arrays.asList(new PartitionRange(1, 1), new PartitionRange(2, 2)), "");
    client.registerShuffle(registerShuffleRequest);
    client.sendShuffleData(request);

    assertEquals(30, shuffleServer.getShuffleTaskManager().getCachedBlockIds(appId, shuffleId).getLongCardinality());
  }

  protected void generateTestData(
      int shuffleId, int partitionId, int num, int length, long taskAttemptId, List<ShuffleBlockInfo> blocks) {
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = (ATOMIC_LONG.getAndIncrement()
                          << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH)) + taskAttemptId;
      blocks.add(new ShuffleBlockInfo(shuffleId, partitionId, blockId, length,
          ChecksumUtils.getCrc32(buf), buf, null, length,0, taskAttemptId));
    }
  }
}
