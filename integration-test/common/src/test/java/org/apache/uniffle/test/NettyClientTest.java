package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NettyClientTest {
  private AtomicLong ATOMIC_LONG = new AtomicLong();
  @Test
  public void test() throws Exception {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
    ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();

    ShuffleServerGrpcNettyClient client = new ShuffleServerGrpcNettyClient("127.0.0.1", 19999, 29999);

    int shuffleId = 0;
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleBlocks = Maps.newHashMap();
    Map<Integer, List<ShuffleBlockInfo>> partBlocks = Maps.newHashMap();
    List<ShuffleBlockInfo> part1Blocks = Lists.newArrayList();
    generateTestData(shuffleId, 1, 10, 100, 0, part1Blocks);
    partBlocks.put(0, part1Blocks);

    Map<Integer, List<ShuffleBlockInfo>> part2 = Maps.newHashMap();
    List<ShuffleBlockInfo> part2Blocks = Lists.newArrayList();
    generateTestData(shuffleId, 2, 20, 100, 0, part1Blocks);
    partBlocks.put(0, part2Blocks);

    shuffleBlocks.put(0, partBlocks);

    String appId = "test_app";
    RssSendShuffleDataRequest request = new RssSendShuffleDataRequest(appId, 1, 10, shuffleBlocks);
    client.sendShuffleData(request);

    RssGetShuffleResultRequest part1Req = new RssGetShuffleResultRequest(appId, shuffleId, 1);
    RssGetShuffleResultResponse part1Resp = client.getShuffleResult(part1Req);
    assertEquals(10, part1Resp.getBlockIdBitmap().getLongCardinality());

    RssGetShuffleResultRequest part2Req = new RssGetShuffleResultRequest(appId, shuffleId, 2);
    RssGetShuffleResultResponse part2Resp = client.getShuffleResult(part2Req);
    assertEquals(20, part2Resp.getBlockIdBitmap().getLongCardinality());
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
