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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.TestUtils;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.RSS_LOCAL_STORAGE_MULTIPLE_DISK_SELECTION_ENABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is to test the {@link org.apache.uniffle.server.storage.local.ChainableLocalStorageSelector} multiple
 * disk selection strategy for reader.
 */
public class ShuffleClientWithLocalMultiDiskTest extends ShuffleReadWriteBase {

  private static ShuffleServerInfo shuffleServerInfo;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;
  private ShuffleReadClientImpl shuffleReadClientImpl;

  @BeforeAll
  public static void setupShuffleServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    System.out.println("base: " + basePath);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setLong(ShuffleServerConf.DISK_CAPACITY, 10 * 1024 * 1024);
    shuffleServerConf.set(RSS_LOCAL_STORAGE_MULTIPLE_DISK_SELECTION_ENABLE, true);
    createShuffleServer(shuffleServerConf);
    startServers();
    shuffleServerInfo =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT);
  }

  @BeforeEach
  public void createClient() {
    shuffleWriteClientImpl = new ShuffleWriteClientImpl(
        ClientType.GRPC.name(), 3, 1000, 1,
        1, 1, 1, true, 1, 1, 10, 10);
  }

  @AfterEach
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @Test
  public void testClientRemoteReadFromMultipleDisk() throws Exception {
    String appId = "testClientRemoteReadFromMultipleDisk_appId";
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo,
        appId,
        0,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL
    );

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // First committing, blocks will be written to one disk
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo));
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(appId, blocks, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(3, result.getSuccessBlockIds().size());

    boolean commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(shuffleServerInfo), appId, 0, 1);
    assertTrue(commitResult);

    // Mark one storage reaching high watermark, it should switch another storage for next writing
    ShuffleServer shuffleServer = shuffleServers.get(0);
    ShuffleDataReadEvent readEvent = new ShuffleDataReadEvent(appId, 0, 0, 0, 0);
    LocalStorage storage1 = (LocalStorage) shuffleServer.getStorageManager().selectStorage(readEvent);
    storage1.getMetaData().setSize(20 * 1024 * 1024);

    blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo));
    result = shuffleWriteClientImpl.sendShuffleData(appId, blocks, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(3, result.getSuccessBlockIds().size());
    commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(shuffleServerInfo), appId, 0, 1);
    assertTrue(commitResult);

    readEvent = new ShuffleDataReadEvent(appId, 0, 0, 0, 1);
    LocalStorage storage2 = (LocalStorage) shuffleServer.getStorageManager().selectStorage(readEvent);
    assertNotEquals(storage1, storage2);

    /**
     * String storageType,
     *       String appId,
     *       int shuffleId,
     *       int partitionId,
     *       int indexReadLimit,
     *       int partitionNumPerRange,
     *       int partitionNum,
     *       int readBufferSize,
     *       String storageBasePath,
     *       Roaring64NavigableMap blockIdBitmap,
     *       Roaring64NavigableMap taskIdBitmap,
     *       List<ShuffleServerInfo> shuffleServerInfoList,
     *       Configuration hadoopConf,
     *       IdHelper idHelper) {
     */
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.LOCALFILE.name(),
        appId,
        0,
        0,
        100,
        1,
        1,
        1000,
        "",
        blockIdBitmap,
        Roaring64NavigableMap.bitmapOf(0),
        Lists.newArrayList(shuffleServerInfo),
        new Configuration(),
        new DefaultIdHelper()
    );

    TestUtils.validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }
}
