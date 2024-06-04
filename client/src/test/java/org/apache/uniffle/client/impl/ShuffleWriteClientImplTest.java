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

package org.apache.uniffle.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.RssUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShuffleWriteClientImplTest {

  @Test
  public void testAbandonEventWhenTaskFailed() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());

    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenAnswer(
            (Answer<String>)
                invocation -> {
                  Thread.sleep(50000);
                  return "ABCD1234";
                });

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));

    // It should directly exit and wont do rpc request.
    Awaitility.await()
        .timeout(1, TimeUnit.SECONDS)
        .until(
            () -> {
              spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> true);
              return true;
            });
  }

  @Test
  public void testSendData() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(new RssSendShuffleDataResponse(StatusCode.NO_BUFFER));

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result =
        spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> false);

    assertTrue(result.getFailedBlockIds().contains(10L));
  }

  @Test
  public void testRegisterAndUnRegisterShuffleServer() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .build();
    String appId1 = "testRegisterAndUnRegisterShuffleServer-1";
    String appId2 = "testRegisterAndUnRegisterShuffleServer-2";
    ShuffleServerInfo server1 = new ShuffleServerInfo("host1-0", "host1", 0);
    ShuffleServerInfo server2 = new ShuffleServerInfo("host2-0", "host2", 0);
    ShuffleServerInfo server3 = new ShuffleServerInfo("host3-0", "host3", 0);
    shuffleWriteClient.addShuffleServer(appId1, 0, server1);
    shuffleWriteClient.addShuffleServer(appId1, 1, server2);
    shuffleWriteClient.addShuffleServer(appId2, 1, server3);
    assertEquals(2, shuffleWriteClient.getAllShuffleServers(appId1).size());
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId2).size());
    shuffleWriteClient.addShuffleServer(appId1, 1, server1);
    shuffleWriteClient.unregisterShuffle(appId1, 1);
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId1).size());
    shuffleWriteClient.unregisterShuffle(appId1);
    assertEquals(0, shuffleWriteClient.getAllShuffleServers(appId1).size());
    shuffleWriteClient.addShuffleServer(appId2, 2, server1);
    assertEquals(2, shuffleWriteClient.getAllShuffleServers(appId2).size());
    shuffleWriteClient.unregisterShuffle(appId2);
    assertEquals(0, shuffleWriteClient.getAllShuffleServers(appId2).size());
  }

  @Test
  public void testSendDataWithDefectiveServers() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(3)
            .replicaWrite(2)
            .replicaRead(2)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.NO_BUFFER),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));

    String appId = "testSendDataWithDefectiveServers_appId";
    ShuffleServerInfo ssi1 = new ShuffleServerInfo("127.0.0.1", 0);
    ShuffleServerInfo ssi2 = new ShuffleServerInfo("127.0.0.1", 1);
    ShuffleServerInfo ssi3 = new ShuffleServerInfo("127.0.0.1", 2);
    List<ShuffleServerInfo> shuffleServerInfoList = Lists.newArrayList(ssi1, ssi2, ssi3);
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result =
        spyClient.sendShuffleData(appId, shuffleBlockInfoList, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());

    // Send data for the second time, the first shuffle server will be moved to the last.
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));
    List<ShuffleServerInfo> excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi2, excludeServers.get(0));
    assertEquals(ssi3, excludeServers.get(1));
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        1,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        false);
    assertEquals(3, excludeServers.size());
    assertEquals(ssi1, excludeServers.get(2));
    result = spyClient.sendShuffleData(appId, shuffleBlockInfoList, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());

    // Send data for the third time, the first server will be removed from the defectiveServers
    // and the second server will be added to the defectiveServers.
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.NO_BUFFER),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));
    List<ShuffleServerInfo> shuffleServerInfoList2 = Lists.newArrayList(ssi2, ssi1, ssi3);
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList2, 10, 100, 0));
    result = spyClient.sendShuffleData(appId, shuffleBlockInfoList2, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(1, spyClient.getDefectiveServers().size());
    assertEquals(ssi2, spyClient.getDefectiveServers().toArray()[0]);
    excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi1, excludeServers.get(0));
    assertEquals(ssi3, excludeServers.get(1));
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        1,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        false);
    assertEquals(3, excludeServers.size());
    assertEquals(ssi2, excludeServers.get(2));

    // Check whether it is normal when two shuffle servers in defectiveServers
    spyClient.getDefectiveServers().add(ssi1);
    assertEquals(2, spyClient.getDefectiveServers().size());
    excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi3, excludeServers.get(0));
    assertEquals(ssi1, excludeServers.get(1));
  }

  @Test
  public void testSettingRssClientConfigs() {
    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.NETTY_IO_MODE, IOMode.EPOLL);
    ShuffleClientFactory.WriteClientBuilder writeClientBuilder =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC_NETTY.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .rssConf(rssConf);
    ShuffleWriteClientImpl client = writeClientBuilder.build();
    IOMode ioMode = writeClientBuilder.getRssConf().get(RssClientConf.NETTY_IO_MODE);
    client.close();
    assertEquals(IOMode.EPOLL, ioMode);
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testGetShuffleResult(BlockIdLayout layout) {
    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, layout.sequenceNoBits);
    rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, layout.partitionIdBits);
    rssConf.set(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, layout.taskAttemptIdBits);
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(60)
            .unregisterRequestTimeSec(10)
            .rssConf(rssConf)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    RssGetShuffleResultResponse response;
    try {
      Roaring64NavigableMap res = Roaring64NavigableMap.bitmapOf(1L, 2L, 5L);
      response = new RssGetShuffleResultResponse(StatusCode.SUCCESS, RssUtils.serializeBitMap(res));
    } catch (Exception e) {
      throw new RssException(e);
    }
    when(mockShuffleServerClient.getShuffleResult(any())).thenReturn(response);

    Set<ShuffleServerInfo> shuffleServerInfoSet =
        Sets.newHashSet(new ShuffleServerInfo("id", "host", 0));
    Roaring64NavigableMap result =
        spyClient.getShuffleResult("GRPC", shuffleServerInfoSet, "appId", 1, 2);

    verify(mockShuffleServerClient)
        .getShuffleResult(argThat(request -> request.getBlockIdLayout().equals(layout)));
    assertArrayEquals(result.stream().sorted().toArray(), new long[] {1L, 2L, 5L});
  }
}
