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

package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.uniffle.client.TestUtils;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ShuffleReadWriteBase extends IntegrationTestBase {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);
  protected List<ShuffleServerInfo> mockSSI =
      Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId, long taskAttemptId,
      int blockNum, int length, Roaring64NavigableMap blockIdBitmap, Map<Long, byte[]> dataMap,
      List<ShuffleServerInfo> shuffleServerInfoList) {
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long seqno = ATOMIC_LONG.getAndIncrement();

      long blockId = (seqno << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH)) + taskAttemptId;
      blockIdBitmap.addLong(blockId);
      dataMap.put(blockId, buf);
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          shuffleId, partitionId, blockId, length, ChecksumUtils.getCrc32(buf),
          buf, shuffleServerInfoList, length, 10, taskAttemptId));
    }
    return shuffleBlockInfoList;
  }

  protected Map<Integer, List<ShuffleBlockInfo>> createTestData(
      Roaring64NavigableMap[] bitmaps,
      Map<Long, byte[]> expectedData) {
    for (int i = 0; i < 4; i++) {
      bitmaps[i] = Roaring64NavigableMap.bitmapOf();
    }
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 0, 3, 25, bitmaps[0], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 1, 1, 5, 25, bitmaps[1], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        0, 2, 2, 4, 25, bitmaps[2], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        0, 3, 3, 1, 25, bitmaps[3], expectedData, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    partitionToBlocks.put(2, blocks3);
    partitionToBlocks.put(3, blocks4);
    return partitionToBlocks;
  }

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId, long taskAttemptId,
      int blockNum, int length, Roaring64NavigableMap blockIdBitmap, Map<Long, byte[]> dataMap) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    return createShuffleBlockList(
        shuffleId, partitionId, taskAttemptId, blockNum, length, blockIdBitmap, dataMap, shuffleServerInfoList);
  }

  protected boolean compareByte(byte[] expected, ByteBuffer buffer) {
    return TestUtils.compareByte(expected, buffer);
  }

  protected void validateResult(ShuffleReadClient readClient, Map<Long, byte[]> expectedData) {
    TestUtils.validateResult(readClient, expectedData);
  }

  protected static String generateBasePath() {
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    tmpDir.deleteOnExit();
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    return basePath;
  }

  protected List<ShuffleDataSegment> readShuffleIndexSegments(
      ShuffleServerGrpcClient shuffleServerClient,
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize) {
    // read index file
    RssGetShuffleIndexRequest rgsir = new RssGetShuffleIndexRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);
    ShuffleIndexResult shuffleIndexResult = shuffleServerClient.getShuffleIndex(rgsir).getShuffleIndexResult();
    return RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);

  }

  protected ShuffleDataResult readShuffleData(
      ShuffleServerGrpcClient shuffleServerClient,
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      int segmentIndex,
      List<ShuffleDataSegment> sds) {
    if (segmentIndex >= sds.size()) {
      return new ShuffleDataResult();
    }

    // read shuffle data
    ShuffleDataSegment segment = sds.get(segmentIndex);
    RssGetShuffleDataRequest rgsdr = new RssGetShuffleDataRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum,
        segment.getOffset(), segment.getLength());

    return new ShuffleDataResult(
        shuffleServerClient.getShuffleData(rgsdr).getShuffleData(),
        segment.getBufferSegments());
  }

  protected ShuffleDataResult readShuffleData(
      ShuffleServerGrpcClient shuffleServerClient,
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      int segmentIndex) {
    // read index file
    RssGetShuffleIndexRequest rgsir = new RssGetShuffleIndexRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);
    ShuffleIndexResult shuffleIndexResult = shuffleServerClient.getShuffleIndex(rgsir).getShuffleIndexResult();
    if (shuffleIndexResult == null) {
      return new ShuffleDataResult();
    }
    List<ShuffleDataSegment> sds = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);

    if (segmentIndex >= sds.size()) {
      return new ShuffleDataResult();
    }

    // read shuffle data
    ShuffleDataSegment segment = sds.get(segmentIndex);
    RssGetShuffleDataRequest rgsdr = new RssGetShuffleDataRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum,
        segment.getOffset(), segment.getLength());

    return new ShuffleDataResult(
        shuffleServerClient.getShuffleData(rgsdr).getShuffleData(),
        segment.getBufferSegments());
  }
}
