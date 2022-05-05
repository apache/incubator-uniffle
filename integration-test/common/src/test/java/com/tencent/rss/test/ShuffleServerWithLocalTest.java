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

package com.tencent.rss.test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ShuffleServerWithLocalTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString("rss.server.app.expired.withoutHeartbeat", "5000");
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void localWriteReadTest() throws Exception {
    String testAppId = "localWriteReadTest";
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, 0,
        Lists.newArrayList(new PartitionRange(0, 1)), "");
    shuffleServerClient.registerShuffle(rrsr);
    rrsr = new RssRegisterShuffleRequest(testAppId, 0, Lists.newArrayList(new PartitionRange(2, 3)), "");
    shuffleServerClient.registerShuffle(rrsr);

    Map<Long, byte[]> expectedData = Maps.newHashMap();

    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[4];
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = createTestData(bitmaps, expectedData);

    Set<Long> expectedBlockIds1 = transBitmapToSet(bitmaps[0]);
    Set<Long> expectedBlockIds2 = transBitmapToSet(bitmaps[1]);
    Set<Long> expectedBlockIds3 = transBitmapToSet(bitmaps[2]);
    Set<Long> expectedBlockIds4 = transBitmapToSet(bitmaps[3]);

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
        testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    RssSendCommitRequest rscr = new RssSendCommitRequest(testAppId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(testAppId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    ShuffleDataResult sdr  = readShuffleData(
        shuffleServerClient, testAppId, 0, 0, 2,
        10, 1000, 0);
    validateResult(sdr, expectedBlockIds1, expectedData, 0);
    sdr  = readShuffleData(
        shuffleServerClient, testAppId, 0, 1, 2,
        10, 1000, 0);
    validateResult(sdr, expectedBlockIds2, expectedData, 1);
    sdr  = readShuffleData(
        shuffleServerClient, testAppId, 0, 2, 2,
        10, 1000, 0);
    validateResult(sdr, expectedBlockIds3, expectedData, 2);
    sdr  = readShuffleData(
        shuffleServerClient, testAppId, 0, 3, 2,
        10, 1000, 0);
    validateResult(sdr, expectedBlockIds4, expectedData, 3);

    assertNotNull(shuffleServers.get(0).getShuffleTaskManager()
        .getPartitionsToBlockIds().get(testAppId));
    Thread.sleep(8000);
    assertNull(shuffleServers.get(0).getShuffleTaskManager().getPartitionsToBlockIds().get(testAppId));
  }

  protected void validateResult(ShuffleDataResult sdr, Set<Long> expectedBlockIds,
      Map<Long, byte[]> expectedData, long expectedTaskAttemptId) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matched = 0;
    for (BufferSegment bs : bufferSegments) {
      if (expectedBlockIds.contains(bs.getBlockId())) {
        byte[] data = new byte[bs.getLength()];
        System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
        assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
        assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
        assertTrue(expectedBlockIds.contains(bs.getBlockId()));
        assertEquals(expectedTaskAttemptId, bs.getTaskAttemptId());
        matched++;
      }
    }
    assertEquals(expectedBlockIds.size(), matched);
  }

  private Set<Long> transBitmapToSet(Roaring64NavigableMap blockIdBitmap) {
    Set<Long> blockIds = Sets.newHashSet();
    LongIterator iter = blockIdBitmap.getLongIterator();
    while (iter.hasNext()) {
      blockIds.add(iter.next());
    }
    return blockIds;
  }
}
