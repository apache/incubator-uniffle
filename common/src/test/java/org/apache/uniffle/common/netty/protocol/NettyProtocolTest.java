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

package org.apache.uniffle.common.netty.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyProtocolTest {
  @Test
  public void testSendShuffleDataRequest() {
    String appId = "test_app";
    byte[] data = new byte[] {1, 2, 3};
    List<ShuffleServerInfo> shuffleServerInfoList =
        Arrays.asList(new ShuffleServerInfo("aaa", 1), new ShuffleServerInfo("bbb", 2));
    List<ShuffleBlockInfo> shuffleBlockInfoList1 =
        Arrays.asList(
            new ShuffleBlockInfo(
                1,
                1,
                1,
                10,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                1,
                10,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1));
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Arrays.asList(
            new ShuffleBlockInfo(
                1,
                2,
                1,
                10,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                2,
                10,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(1, shuffleBlockInfoList1);
    partitionToBlocks.put(2, shuffleBlockInfoList2);
    SendShuffleDataRequest sendShuffleDataRequest =
        new SendShuffleDataRequest(1L, appId, 1, 1, partitionToBlocks, 12345);
    int encodeLength = sendShuffleDataRequest.encodedLength();

    ByteBuf byteBuf = Unpooled.buffer(sendShuffleDataRequest.encodedLength());
    sendShuffleDataRequest.encode(byteBuf);
    assertEquals(byteBuf.readableBytes(), encodeLength);
    SendShuffleDataRequest sendShuffleDataRequest1 = sendShuffleDataRequest.decode(byteBuf);
    assertTrue(
        NettyProtocolTestUtils.compareSendShuffleDataRequest(
            sendShuffleDataRequest, sendShuffleDataRequest1));
    assertEquals(encodeLength, sendShuffleDataRequest1.encodedLength());
    byteBuf.release();
    for (ShuffleBlockInfo shuffleBlockInfo :
        sendShuffleDataRequest1.getPartitionToBlocks().get(1)) {
      shuffleBlockInfo.getData().release();
    }
    for (ShuffleBlockInfo shuffleBlockInfo :
        sendShuffleDataRequest1.getPartitionToBlocks().get(2)) {
      shuffleBlockInfo.getData().release();
    }
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testRpcResponse() {
    RpcResponse rpcResponse = new RpcResponse(1, StatusCode.SUCCESS, "test_message");
    int encodeLength = rpcResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength);
    rpcResponse.encode(byteBuf);
    assertEquals(byteBuf.readableBytes(), encodeLength);
    RpcResponse rpcResponse1 = RpcResponse.decode(byteBuf);
    assertEquals(rpcResponse.getRequestId(), rpcResponse1.getRequestId());
    assertEquals(rpcResponse.getRetMessage(), rpcResponse1.getRetMessage());
    assertEquals(rpcResponse.getStatusCode(), rpcResponse1.getStatusCode());
    assertEquals(rpcResponse.encodedLength(), rpcResponse1.encodedLength());
    byteBuf.release();
  }

  @Test
  public void testGetLocalShuffleDataRequest() {
    GetLocalShuffleDataRequest getLocalShuffleDataRequest =
        new GetLocalShuffleDataRequest(
            1, "test_app", 1, 1, 1, 100, 0, 200, System.currentTimeMillis());
    int encodeLength = getLocalShuffleDataRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleDataRequest.encode(byteBuf);
    GetLocalShuffleDataRequest getLocalShuffleDataRequest1 =
        GetLocalShuffleDataRequest.decode(byteBuf);

    assertEquals(
        getLocalShuffleDataRequest.getRequestId(), getLocalShuffleDataRequest1.getRequestId());
    assertEquals(getLocalShuffleDataRequest.getAppId(), getLocalShuffleDataRequest1.getAppId());
    assertEquals(
        getLocalShuffleDataRequest.getShuffleId(), getLocalShuffleDataRequest1.getShuffleId());
    assertEquals(
        getLocalShuffleDataRequest.getPartitionId(), getLocalShuffleDataRequest1.getPartitionId());
    assertEquals(
        getLocalShuffleDataRequest.getPartitionNumPerRange(),
        getLocalShuffleDataRequest1.getPartitionNumPerRange());
    assertEquals(
        getLocalShuffleDataRequest.getPartitionNum(),
        getLocalShuffleDataRequest1.getPartitionNum());
    assertEquals(getLocalShuffleDataRequest.getOffset(), getLocalShuffleDataRequest1.getOffset());
    assertEquals(getLocalShuffleDataRequest.getLength(), getLocalShuffleDataRequest1.getLength());
    assertEquals(
        getLocalShuffleDataRequest.getTimestamp(), getLocalShuffleDataRequest1.getTimestamp());
  }

  @Test
  public void testGetLocalShuffleDataResponse() {
    byte[] data = new byte[] {1, 2, 3};
    GetLocalShuffleDataResponse getLocalShuffleDataResponse =
        new GetLocalShuffleDataResponse(
            1,
            StatusCode.SUCCESS,
            "",
            new NettyManagedBuffer(Unpooled.wrappedBuffer(data).retain()));
    int encodeLength = getLocalShuffleDataResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleDataResponse.encode(byteBuf);
    GetLocalShuffleDataResponse getLocalShuffleDataResponse1 =
        GetLocalShuffleDataResponse.decode(byteBuf);

    assertEquals(
        getLocalShuffleDataResponse.getRequestId(), getLocalShuffleDataResponse1.getRequestId());
    assertEquals(
        getLocalShuffleDataResponse.getRetMessage(), getLocalShuffleDataResponse1.getRetMessage());
    assertEquals(
        getLocalShuffleDataResponse.getStatusCode(), getLocalShuffleDataResponse1.getStatusCode());
    assertEquals(getLocalShuffleDataResponse.getData(), getLocalShuffleDataResponse1.getData());
  }

  @Test
  public void testGetLocalShuffleIndexRequest() {
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest =
        new GetLocalShuffleIndexRequest(1, "test_app", 1, 1, 1, 100);
    int encodeLength = getLocalShuffleIndexRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleIndexRequest.encode(byteBuf);
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest1 =
        GetLocalShuffleIndexRequest.decode(byteBuf);

    assertEquals(
        getLocalShuffleIndexRequest.getRequestId(), getLocalShuffleIndexRequest1.getRequestId());
    assertEquals(getLocalShuffleIndexRequest.getAppId(), getLocalShuffleIndexRequest1.getAppId());
    assertEquals(
        getLocalShuffleIndexRequest.getShuffleId(), getLocalShuffleIndexRequest1.getShuffleId());
    assertEquals(
        getLocalShuffleIndexRequest.getPartitionId(),
        getLocalShuffleIndexRequest1.getPartitionId());
    assertEquals(
        getLocalShuffleIndexRequest.getPartitionNumPerRange(),
        getLocalShuffleIndexRequest1.getPartitionNumPerRange());
    assertEquals(
        getLocalShuffleIndexRequest.getPartitionNum(),
        getLocalShuffleIndexRequest1.getPartitionNum());
  }

  @Test
  public void testGetLocalShuffleIndexResponse() {
    byte[] indexData = new byte[] {1, 2, 3};
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse =
        new GetLocalShuffleIndexResponse(
            1, StatusCode.SUCCESS, "", Unpooled.wrappedBuffer(indexData).retain(), 23);
    int encodeLength = getLocalShuffleIndexResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleIndexResponse.encode(byteBuf);
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse1 =
        GetLocalShuffleIndexResponse.decode(byteBuf);

    assertEquals(
        getLocalShuffleIndexResponse.getRequestId(), getLocalShuffleIndexResponse1.getRequestId());
    assertEquals(
        getLocalShuffleIndexResponse.getStatusCode(),
        getLocalShuffleIndexResponse1.getStatusCode());
    assertEquals(
        getLocalShuffleIndexResponse.getRetMessage(),
        getLocalShuffleIndexResponse1.getRetMessage());
    assertEquals(
        getLocalShuffleIndexResponse.getFileLength(),
        getLocalShuffleIndexResponse1.getFileLength());
    assertEquals(
        getLocalShuffleIndexResponse.getIndexData(), getLocalShuffleIndexResponse1.getIndexData());
  }

  @Test
  public void testGetMemoryShuffleDataRequest() {
    Roaring64NavigableMap expectedTaskIdsBitmap = Roaring64NavigableMap.bitmapOf(1, 2, 3, 4, 5);
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest =
        new GetMemoryShuffleDataRequest(
            1, "test_app", 1, 1, 1, 64, System.currentTimeMillis(), expectedTaskIdsBitmap);
    int encodeLength = getMemoryShuffleDataRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getMemoryShuffleDataRequest.encode(byteBuf);
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest1 =
        GetMemoryShuffleDataRequest.decode(byteBuf);

    assertEquals(
        getMemoryShuffleDataRequest.getRequestId(), getMemoryShuffleDataRequest1.getRequestId());
    assertEquals(getMemoryShuffleDataRequest.getAppId(), getMemoryShuffleDataRequest1.getAppId());
    assertEquals(
        getMemoryShuffleDataRequest.getShuffleId(), getMemoryShuffleDataRequest1.getShuffleId());
    assertEquals(
        getMemoryShuffleDataRequest.getPartitionId(),
        getMemoryShuffleDataRequest1.getPartitionId());
    assertEquals(
        getMemoryShuffleDataRequest.getLastBlockId(),
        getMemoryShuffleDataRequest1.getLastBlockId());
    assertEquals(
        getMemoryShuffleDataRequest.getReadBufferSize(),
        getMemoryShuffleDataRequest1.getReadBufferSize());
    assertEquals(
        getMemoryShuffleDataRequest.getTimestamp(), getMemoryShuffleDataRequest1.getTimestamp());
    assertEquals(
        getMemoryShuffleDataRequest.getExpectedTaskIdsBitmap().getLongCardinality(),
        getMemoryShuffleDataRequest1.getExpectedTaskIdsBitmap().getLongCardinality());
  }

  @Test
  public void testGetMemoryShuffleDataResponse() {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    List<BufferSegment> bufferSegments =
        Lists.newArrayList(
            new BufferSegment(1, 0, 5, 10, 123, 1), new BufferSegment(1, 0, 5, 10, 345, 1));
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse =
        new GetMemoryShuffleDataResponse(
            1, StatusCode.SUCCESS, "", bufferSegments, Unpooled.wrappedBuffer(data).retain());
    int encodeLength = getMemoryShuffleDataResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getMemoryShuffleDataResponse.encode(byteBuf);
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse1 =
        GetMemoryShuffleDataResponse.decode(byteBuf);

    assertEquals(
        getMemoryShuffleDataResponse.getRequestId(), getMemoryShuffleDataResponse1.getRequestId());
    assertEquals(
        getMemoryShuffleDataResponse.getStatusCode(),
        getMemoryShuffleDataResponse1.getStatusCode());
    assertTrue(
        getMemoryShuffleDataResponse.getData().equals(getMemoryShuffleDataResponse1.getData()));

    for (int i = 0; i < 2; i++) {
      assertEquals(
          getMemoryShuffleDataResponse.getBufferSegments().get(i),
          getMemoryShuffleDataResponse1.getBufferSegments().get(i));
    }
  }
}
