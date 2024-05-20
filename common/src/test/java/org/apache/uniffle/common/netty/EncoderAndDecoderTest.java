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

package org.apache.uniffle.common.netty;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.protocol.NettyProtocolTestUtils;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.common.util.OpaqueBlockId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EncoderAndDecoderTest {
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;
  private static final SendShuffleDataRequest DATA_REQUEST = generateShuffleDataRequest();
  private static final String EXPECTED_MESSAGE = "test_message";
  private static final long REQUEST_ID = 1;
  private static final StatusCode STATUS_CODE = StatusCode.SUCCESS;
  private static final int PORT = 12345;
  private static final BlockId blockId1 = new OpaqueBlockId(1);
  private static final BlockId blockId2 = new OpaqueBlockId(2);

  static class MockResponseHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof RpcResponse) {
        RpcResponse rpcResponse = (RpcResponse) msg;
        assertEquals(REQUEST_ID, rpcResponse.getRequestId());
        assertEquals(STATUS_CODE, rpcResponse.getStatusCode());
        assertEquals(EXPECTED_MESSAGE, rpcResponse.getRetMessage());
      } else if (msg instanceof SendShuffleDataRequest) {
        SendShuffleDataRequest sendShuffleDataRequest = (SendShuffleDataRequest) msg;
        assertTrue(
            NettyProtocolTestUtils.compareSendShuffleDataRequest(
                sendShuffleDataRequest, DATA_REQUEST));
        sendShuffleDataRequest.getPartitionToBlocks().values().stream()
            .flatMap(Collection::stream)
            .forEach(shuffleBlockInfo -> shuffleBlockInfo.getData().release());
        sendShuffleDataRequest.getPartitionToBlocks().values().stream()
            .flatMap(Collection::stream)
            .forEach(shuffleBlockInfo -> assertEquals(0, shuffleBlockInfo.getData().refCnt()));
        RpcResponse rpcResponse = new RpcResponse(REQUEST_ID, STATUS_CODE, EXPECTED_MESSAGE);
        ctx.writeAndFlush(rpcResponse);
      } else {
        throw new RssException("receive unexpected message!");
      }
      super.channelRead(ctx, msg);
    }
  }

  @Test
  public void test() throws InterruptedException {
    EventLoopGroup workerGroup = NettyUtils.createEventLoop(IOMode.NIO, 2, "netty-client");
    PooledByteBufAllocator pooledByteBufAllocator =
        NettyUtils.createPooledByteBufAllocator(true, false /* allowCache */, 2);
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(workerGroup)
        .channel(NettyUtils.getClientChannelClass(IOMode.NIO))
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
        .option(ChannelOption.ALLOCATOR, pooledByteBufAllocator);
    final AtomicReference<Channel> channelRef = new AtomicReference<>();
    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            ch.pipeline()
                .addLast("ClientEncoder", MessageEncoder.INSTANCE)
                .addLast("ClientDecoder", new TransportFrameDecoder())
                .addLast("ClientResponseHandler", new MockResponseHandler());
            channelRef.set(ch);
          }
        });
    bootstrap.connect("localhost", PORT);
    // wait for initChannel
    Thread.sleep(200);
    channelRef.get().writeAndFlush(DATA_REQUEST);
    channelRef.get().closeFuture().await(3L, TimeUnit.SECONDS);
    DATA_REQUEST.getPartitionToBlocks().values().stream()
        .flatMap(Collection::stream)
        .forEach(shuffleBlockInfo -> shuffleBlockInfo.getData().release());
  }

  private static SendShuffleDataRequest generateShuffleDataRequest() {
    String appId = "test_app";
    byte[] data = new byte[] {1, 2, 3};
    List<ShuffleServerInfo> shuffleServerInfoList =
        Arrays.asList(new ShuffleServerInfo("aaa", 1), new ShuffleServerInfo("bbb", 2));
    List<ShuffleBlockInfo> shuffleBlockInfoList1 =
        Arrays.asList(
            new ShuffleBlockInfo(
                1,
                1,
                blockId1,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                blockId1,
                data.length,
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
                blockId1,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                blockId2,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(1, shuffleBlockInfoList1);
    partitionToBlocks.put(2, shuffleBlockInfoList2);
    return new SendShuffleDataRequest(1L, appId, 1, 1, partitionToBlocks, 12345);
  }

  @BeforeEach
  public void startNettyServer() {
    bossGroup = NettyUtils.createEventLoop(IOMode.NIO, 1, "netty-boss-group");
    workerGroup = NettyUtils.createEventLoop(IOMode.NIO, 5, "netty-worker-group");
    ServerBootstrap serverBootstrap =
        new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);
    serverBootstrap
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(final SocketChannel ch) {
                ch.pipeline()
                    .addLast("ServerEncoder", MessageEncoder.INSTANCE)
                    .addLast("ServerDecoder", new TransportFrameDecoder())
                    .addLast("ServerResponseHandler", new MockResponseHandler());
              }
            })
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true);
    channelFuture = serverBootstrap.bind(PORT);
    channelFuture.syncUninterruptibly();
  }

  @AfterEach
  public void stopNettyServer() {
    channelFuture.channel().close().awaitUninterruptibly(10L, TimeUnit.SECONDS);
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}
