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

package org.apache.uniffle.common.util;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.netty.protocol.Message;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NettyUtilsTest {
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;
  private static final String EXPECTED_MESSAGE = "test_message";
  private static final int PORT = 12345;

  static class MockDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list)
        throws Exception {
      RpcResponse rpcResponse = new RpcResponse(1L, StatusCode.SUCCESS, EXPECTED_MESSAGE);
      NettyUtils.writeResponseMsg(ctx, rpcResponse, true);
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
                .addLast(
                    new ByteToMessageDecoder() {
                      @Override
                      protected void decode(
                          ChannelHandlerContext channelHandlerContext,
                          ByteBuf byteBuf,
                          List<Object> list) {
                        Message.Type messageType;
                        messageType = Message.Type.decode(byteBuf);
                        assertEquals(Message.Type.RPC_RESPONSE, messageType);
                        RpcResponse rpcResponse =
                            (RpcResponse) Message.decode(messageType, byteBuf);
                        assertEquals(1L, rpcResponse.getRequestId());
                        assertEquals(StatusCode.SUCCESS, rpcResponse.getStatusCode());
                        assertEquals(EXPECTED_MESSAGE, rpcResponse.getRetMessage());
                      }
                    });
            channelRef.set(ch);
          }
        });
    bootstrap.connect("localhost", PORT);
    ByteBuf byteBuf = Unpooled.buffer(1);
    byteBuf.writeByte(1);
    // wait for initChannel
    Thread.sleep(200);
    channelRef.get().writeAndFlush(byteBuf);
    channelRef.get().closeFuture().await(3L, TimeUnit.SECONDS);
  }

  @BeforeEach
  public void startNettyServer() {
    Supplier<ChannelHandler[]> handlerSupplier = () -> new ChannelHandler[] {new MockDecoder()};
    bossGroup = NettyUtils.createEventLoop(IOMode.NIO, 1, "netty-boss-group");
    workerGroup = NettyUtils.createEventLoop(IOMode.NIO, 5, "netty-worker-group");
    ServerBootstrap serverBootstrap =
        new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);
    serverBootstrap
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(final SocketChannel ch) {
                ch.pipeline().addLast(handlerSupplier.get());
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
