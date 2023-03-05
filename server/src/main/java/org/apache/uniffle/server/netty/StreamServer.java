/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.uniffle.server.netty;

import java.util.function.Supplier;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.netty.decoder.StreamServerInitDecoder;

public class StreamServer {

  private static final Logger LOG = LoggerFactory.getLogger(StreamServer.class);

  private ShuffleServer shuffleServer;
  private EventLoopGroup shuffleBossGroup;
  private EventLoopGroup shuffleWorkerGroup;
  private ShuffleServerConf ssc;

  public StreamServer(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.ssc = shuffleServer.getShuffleServerConf();
    boolean isEpollEnable = ssc.getBoolean(ShuffleServerConf.SERVER_UPLOAD_EPOLL_ENABLE);
    int acceptThreads = ssc.getInteger(ShuffleServerConf.SERVER_UPLOAD_ACCEPT_THREAD);
    int workerThreads = ssc.getInteger(ShuffleServerConf.SERVER_UPLOAD_WORKER_THREAD);
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
      Supplier<ChannelHandler[]> handlerSupplier) {
    ServerBootstrap serverBootstrap = bossGroup instanceof EpollEventLoopGroup
        ? new ServerBootstrap().group(bossGroup, workerGroup).channel(EpollServerSocketChannel.class)
        : new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);

    return serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(final SocketChannel ch) {
        ch.pipeline().addLast(handlerSupplier.get());
      }
    })
        .option(ChannelOption.SO_BACKLOG, backlogSize)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  public void start() throws InterruptedException {
    Supplier<ChannelHandler[]> streamHandlers = () -> new ChannelHandler[]{
        new StreamServerInitDecoder(shuffleServer)
    };
    ServerBootstrap streamServerBootstrap = bootstrapChannel(shuffleBossGroup, shuffleWorkerGroup,
        ssc.getInteger(ShuffleServerConf.SERVER_UPLOAD_CONNECT_BACKLOG),
        ssc.getInteger(ShuffleServerConf.SERVER_UPLOAD_CONNECT_TIMEOUT), streamHandlers);

    // Bind the ports and save the results so that the channels can be closed later.
    // If the second bind fails, the first one gets cleaned up in the shutdown.
    int port = ssc.getInteger(ShuffleServerConf.SERVER_UPLOAD_PORT);
    Channel channel =  streamServerBootstrap.bind(port).sync().channel();
    LOG.info("bind localAddress is " + channel.localAddress());
    LOG.info("Start stream server successfully with port " + port);
  }
}
