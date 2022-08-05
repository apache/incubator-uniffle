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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.impl.ComposedClientReadHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileQuorumClientReadHandler;
import org.apache.uniffle.storage.handler.impl.MemoryQuorumClientReadHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerWithMemoryTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir = new File(tmpDir, "data");
    String basePath = dataDir.getAbsolutePath();
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 40.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 500L);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @AfterEach
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void memoryWriteReadTest() throws Exception {
    String testAppId = "memoryWriteReadTest";
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)), "");
    shuffleServerClient.registerShuffle(rrsr);
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Long, byte[]> dataMap = Maps.newHashMap();
    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[1];
    bitmaps[0] = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        shuffleId, partitionId, 0, 3, 25,
      expectBlockIds, dataMap, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    // send data to shuffle server
    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
        testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);

    // data is cached
    assertEquals(3, shuffleServers.get(0).getShuffleBufferManager()
        .getShuffleBuffer(testAppId, shuffleId, 0).getBlocks().size());
    // create memory handler to read data,
    MemoryQuorumClientReadHandler memoryQuorumClientReadHandler = new MemoryQuorumClientReadHandler(
        testAppId, shuffleId, partitionId, 20, Lists.newArrayList(shuffleServerClient));
    // start to read data, one block data for every call
    ShuffleDataResult sdr  = memoryQuorumClientReadHandler.readShuffleData();
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.put(blocks.get(0).getBlockId(), blocks.get(0).getData());
    validateResult(expectedData, sdr);

    sdr = memoryQuorumClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(1).getBlockId(), blocks.get(1).getData());
    validateResult(expectedData, sdr);

    sdr = memoryQuorumClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(2).getBlockId(), blocks.get(2).getData());
    validateResult(expectedData, sdr);

    // no data in cache, empty return
    sdr = memoryQuorumClientReadHandler.readShuffleData();
    assertEquals(0, sdr.getBufferSegments().size());

    // case: read with ComposedClientReadHandler
    memoryQuorumClientReadHandler = new MemoryQuorumClientReadHandler(
        testAppId, shuffleId, partitionId, 50, Lists.newArrayList(shuffleServerClient));
    LocalFileQuorumClientReadHandler localFileQuorumClientReadHandler = new LocalFileQuorumClientReadHandler(
        testAppId, shuffleId, partitionId, 0, 1, 3,
        50, expectBlockIds, processBlockIds, Lists.newArrayList(shuffleServerClient));
    ClientReadHandler[] handlers = new ClientReadHandler[2];
    handlers[0] = memoryQuorumClientReadHandler;
    handlers[1] = localFileQuorumClientReadHandler;
    ComposedClientReadHandler composedClientReadHandler = new ComposedClientReadHandler(handlers);
    // read from memory with ComposedClientReadHandler
    sdr  = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(0).getBlockId(), blocks.get(0).getData());
    expectedData.put(blocks.get(1).getBlockId(), blocks.get(1).getData());
    validateResult(expectedData, sdr);

    // send data to shuffle server, flush should happen
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
      shuffleId, partitionId, 0, 3, 50,
      expectBlockIds, dataMap, mockSSI);
    processBlockIds.addLong(blocks.get(0).getBlockId());
    processBlockIds.addLong(blocks.get(1).getBlockId());

    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks2);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    rssdr = new RssSendShuffleDataRequest(
        testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    // wait until flush finished
    int retry = 0;
    while (true) {
      if (retry > 5) {
        fail("Timeout for flush data");
      }
      ShuffleBuffer shuffleBuffer = shuffleServers.get(0).getShuffleBufferManager()
          .getShuffleBuffer(testAppId, shuffleId, 0);
      if (shuffleBuffer.getBlocks().size() == 0 && shuffleBuffer.getInFlushBlockMap().size() == 0) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }

    // when segment filter is introduced, there is no need to read duplicated data
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(2).getBlockId(), blocks.get(2).getData());
    expectedData.put(blocks2.get(0).getBlockId(), blocks2.get(0).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks.get(2).getBlockId());
    processBlockIds.addLong(blocks2.get(0).getBlockId());

    sdr  = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(1).getBlockId(), blocks2.get(1).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(1).getBlockId());

    sdr  = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(2).getBlockId(), blocks2.get(2).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(2).getBlockId());

    sdr  = composedClientReadHandler.readShuffleData();
    assertNull(sdr);
  }

  @Test
  public void memoryAndLocalFileReadWithFilterTest() throws Exception {
    String testAppId = "memoryAndLocalFileReadWithFilterTest";
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(testAppId, 0,
      Lists.newArrayList(new PartitionRange(0, 0)), "");
    shuffleServerClient.registerShuffle(rrsr);
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Long, byte[]> dataMap = Maps.newHashMap();
    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[1];
    bitmaps[0] = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
      shuffleId, partitionId, 0, 3, 25,
      expectBlockIds, dataMap, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    // send data to shuffle server's memory
    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
      testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);

    // read the 1-th segment from memory
    MemoryQuorumClientReadHandler memoryQuorumClientReadHandler = new MemoryQuorumClientReadHandler(
      testAppId, shuffleId, partitionId, 150, Lists.newArrayList(shuffleServerClient));
    LocalFileQuorumClientReadHandler localFileQuorumClientReadHandler = new LocalFileQuorumClientReadHandler(
      testAppId, shuffleId, partitionId, 0, 1, 3,
      75, expectBlockIds, processBlockIds, Lists.newArrayList(shuffleServerClient));
    ClientReadHandler[] handlers = new ClientReadHandler[2];
    handlers[0] = memoryQuorumClientReadHandler;
    handlers[1] = localFileQuorumClientReadHandler;
    ComposedClientReadHandler composedClientReadHandler = new ComposedClientReadHandler(handlers);
    ShuffleDataResult sdr  = composedClientReadHandler.readShuffleData();
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.clear();
    expectedData.put(blocks.get(0).getBlockId(), blocks.get(0).getData());
    expectedData.put(blocks.get(1).getBlockId(), blocks.get(1).getData());
    expectedData.put(blocks.get(2).getBlockId(), blocks.get(1).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks.get(0).getBlockId());
    processBlockIds.addLong(blocks.get(1).getBlockId());
    processBlockIds.addLong(blocks.get(2).getBlockId());

    // send data to shuffle server, and wait until flush finish
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
      shuffleId, partitionId, 0, 3, 50,
      expectBlockIds, dataMap, mockSSI);
    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks2);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(
      testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);

    int retry = 0;
    while (true) {
      if (retry > 5) {
        fail("Timeout for flush data");
      }
      ShuffleBuffer shuffleBuffer = shuffleServers.get(0).getShuffleBufferManager()
        .getShuffleBuffer(testAppId, shuffleId, 0);
      if (shuffleBuffer.getBlocks().size() == 0 && shuffleBuffer.getInFlushBlockMap().size() == 0) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }

    // read the 2-th segment from localFile
    // notice: the 1-th segment is skipped, because it is processed
    sdr  = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(0).getBlockId(), blocks2.get(0).getData());
    expectedData.put(blocks2.get(1).getBlockId(), blocks2.get(1).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(0).getBlockId());
    processBlockIds.addLong(blocks2.get(1).getBlockId());

    // read the 3-th segment from localFile
    sdr  = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(2).getBlockId(), blocks2.get(2).getData());
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(2).getBlockId());

    // all segments are processed
    sdr  = composedClientReadHandler.readShuffleData();
    assertNull(sdr);
  }

  protected void validateResult(
      Map<Long, byte[]> expectedData,
      ShuffleDataResult sdr) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    assertEquals(expectedData.size(), bufferSegments.size());
    for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
      BufferSegment bs = findBufferSegment(entry.getKey(), bufferSegments);
      assertNotNull(bs);
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
    }
  }

  private BufferSegment findBufferSegment(long blockId, List<BufferSegment> bufferSegments) {
    for (BufferSegment bs : bufferSegments) {
      if (bs.getBlockId() == blockId) {
        return bs;
      }
    }
    return null;
  }
}
