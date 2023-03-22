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

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyProtocolTest {
  @Test
  public void testSendShuffleDataRequest() {
    String appId = "test_app";
    byte[] data = new byte[]{1, 2, 3};
    List<ShuffleServerInfo> shuffleServerInfoList = Arrays.asList(new ShuffleServerInfo("aaa", 1),
        new ShuffleServerInfo("bbb", 2));
    List<ShuffleBlockInfo> shuffleBlockInfoList1 =
        Arrays.asList(new ShuffleBlockInfo(1, 1, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1),
            new ShuffleBlockInfo(1, 1, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1));
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Arrays.asList(new ShuffleBlockInfo(1, 2, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1),
            new ShuffleBlockInfo(1, 1, 2, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1));
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
    assertTrue(NettyProtocolTestUtils.compareSendShuffleDataRequest(sendShuffleDataRequest, sendShuffleDataRequest1));
    assertEquals(encodeLength, sendShuffleDataRequest1.encodedLength());
    byteBuf.release();
    for (ShuffleBlockInfo shuffleBlockInfo : sendShuffleDataRequest1.getPartitionToBlocks().get(1)) {
      shuffleBlockInfo.getData().release();
    }
    for (ShuffleBlockInfo shuffleBlockInfo : sendShuffleDataRequest1.getPartitionToBlocks().get(2)) {
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
    assertTrue(rpcResponse.equals(rpcResponse1));
    assertEquals(rpcResponse.encodedLength(), rpcResponse1.encodedLength());
    byteBuf.release();
  }
}
