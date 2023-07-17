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

package org.apache.uniffle.server.netty;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.TransportFrameDecoder;
import org.apache.uniffle.common.netty.client.TransportConf;
import org.apache.uniffle.common.netty.client.TransportContext;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

public class StreamServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(StreamServer.class);

  private ShuffleServer shuffleServer;
  private EventLoopGroup shuffleBossGroup;
  private EventLoopGroup shuffleWorkerGroup;
  private ShuffleServerConf shuffleServerConf;
  private ChannelFuture channelFuture;

  public StreamServer(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServer.getShuffleServerConf();
    boolean isEpollEnable =
        shuffleServerConf.getBoolean(ShuffleServerConf.NETTY_SERVER_EPOLL_ENABLE);
    int acceptThreads = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_ACCEPT_THREAD);
    int workerThreads = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_WORKER_THREAD);
    if (isEpollEnable) {
      shuffleBossGroup = new EpollEventLoopGroup(acceptThreads);
      shuffleWorkerGroup = new EpollEventLoopGroup(workerThreads);
    } else {
      shuffleBossGroup = new NioEventLoopGroup(acceptThreads);
      shuffleWorkerGroup = new NioEventLoopGroup(workerThreads);
    }
  }

  private ServerBootstrap bootstrapChannel(
      EventLoopGroup bossGroup,
      EventLoopGroup workerGroup,
      int backlogSize,
      int timeoutMillis,
      int sendBuf,
      int receiveBuf) {
    ServerBootstrap serverBootstrap = new ServerBootstrap().group(bossGroup, workerGroup);
    if (bossGroup instanceof EpollEventLoopGroup) {
      serverBootstrap.channel(EpollServerSocketChannel.class);
    } else {
      serverBootstrap.channel(NioServerSocketChannel.class);
    }

    ShuffleServerNettyHandler serverNettyHandler = new ShuffleServerNettyHandler(shuffleServer);
    TransportContext transportContext =
        new TransportContext(new TransportConf(shuffleServerConf), serverNettyHandler, true);
    serverBootstrap
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(final SocketChannel ch) {
                transportContext.initializePipeline(ch, new TransportFrameDecoder());
              }
            })
        .option(ChannelOption.SO_BACKLOG, backlogSize)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    if (sendBuf > 0) {
      serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sendBuf);
    }
    if (receiveBuf > 0) {
      serverBootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBuf);
    }
    return serverBootstrap;
  }

  @Override
  public int start() throws IOException {
    int port = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_PORT);
    try {
      port =
          RssUtils.startServiceOnPort(
              this, Constants.NETTY_STREAM_SERVICE_NAME, port, shuffleServerConf);
    } catch (Exception e) {
      ExitUtils.terminate(1, "Fail to start stream server", e, LOG);
    }
    return port;
  }

  @Override
  public void startOnPort(int port) throws Exception {

    ServerBootstrap serverBootstrap =
        bootstrapChannel(
            shuffleBossGroup,
            shuffleWorkerGroup,
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_CONNECT_BACKLOG),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_CONNECT_TIMEOUT),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_SEND_BUF),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_RECEIVE_BUF));

    // Bind the ports and save the results so that the channels can be closed later.
    // If the second bind fails, the first one gets cleaned up in the shutdown.
    try {
      channelFuture = serverBootstrap.bind(port);
      channelFuture.syncUninterruptibly();
      LOG.info("bind localAddress is " + channelFuture.channel().localAddress());
      LOG.info("Start stream server successfully with port " + port);
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void stop() {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly(10L, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (shuffleBossGroup != null) {
      shuffleBossGroup.shutdownGracefully();
      shuffleWorkerGroup.shutdownGracefully();
      shuffleBossGroup = null;
      shuffleWorkerGroup = null;
    }
  }

  @Override
  public void blockUntilShutdown() throws InterruptedException {}
}
