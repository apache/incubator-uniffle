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

package org.apache.uniffle.common.netty.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.AfterAll;

public abstract class TransportClientTestBase {

  protected static List<MockServer> mockServers = Lists.newArrayList();

  protected static void startMockServer() {
    for (MockServer shuffleServer : mockServers) {
      try {
        shuffleServer.start();
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("start mock server on port %s failed", shuffleServer.port), e);
      }
    }
  }

  @AfterAll
  public static void shutdownServers() throws Exception {
    for (MockServer shuffleServer : mockServers) {
      shuffleServer.stop();
    }
    mockServers.clear();
  }

  public static class MockServer {
    ServerBootstrap bootstrap;
    ChannelFuture channelFuture;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    int port;

    public MockServer(int port) {
      this.port = port;
      this.bossGroup = new NioEventLoopGroup(1);
      this.workerGroup = new NioEventLoopGroup(2);
    }

    public void start() throws IOException {

      try {
        bootstrap = new ServerBootstrap();
        bootstrap
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                  @Override
                  public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new MockEchoServerHandler());
                  }
                });
        channelFuture = bootstrap.bind(port).sync();
      } catch (InterruptedException e) {
        stop();
      }
    }

    public void stop() {
      if (channelFuture != null) {
        channelFuture.channel().close().awaitUninterruptibly(10L, TimeUnit.SECONDS);
        channelFuture = null;
      }
      if (bossGroup != null) {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        bossGroup = null;
        workerGroup = null;
      }
    }
  }

  static class MockEchoServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ctx.writeAndFlush(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }
}
