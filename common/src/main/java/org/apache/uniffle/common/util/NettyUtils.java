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

import java.util.concurrent.ThreadFactory;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.netty.protocol.Message;

public class NettyUtils {
  private static final Logger logger = LoggerFactory.getLogger(NettyUtils.class);

  private static final long MAX_DIRECT_MEMORY_IN_BYTES = PlatformDependent.maxDirectMemory();

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = ThreadUtils.getNettyThreadFactory(threadPrefix);

    switch (mode) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs, boolean allowCache, int numCores) {
    if (numCores == 0) {
      numCores = Runtime.getRuntime().availableProcessors();
    }
    return new PooledByteBufAllocator(
        allowDirectBufs && PlatformDependent.directBufferPreferred(),
        Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
        Math.min(PooledByteBufAllocator.defaultNumDirectArena(), allowDirectBufs ? numCores : 0),
        PooledByteBufAllocator.defaultPageSize(),
        PooledByteBufAllocator.defaultMaxOrder(),
        allowCache ? PooledByteBufAllocator.defaultSmallCacheSize() : 0,
        allowCache ? PooledByteBufAllocator.defaultNormalCacheSize() : 0,
        allowCache && PooledByteBufAllocator.defaultUseCacheForAllThreads());
  }

  /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists. */
  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "<unknown remote>";
  }

  public static ChannelFuture writeResponseMsg(
      ChannelHandlerContext ctx, Message msg, boolean doWriteType) {
    ByteBuf responseMsgBuf = ctx.alloc().buffer(msg.encodedLength());
    try {
      if (doWriteType) {
        responseMsgBuf.writeByte(msg.type().id());
      }
      msg.encode(responseMsgBuf);
      return ctx.writeAndFlush(responseMsgBuf);
    } catch (Throwable ex) {
      logger.warn("Caught exception, releasing ByteBuf", ex);
      responseMsgBuf.release();
      throw ex;
    }
  }

  public static String getServerConnectionInfo(ChannelHandlerContext ctx) {
    return getServerConnectionInfo(ctx.channel());
  }

  public static String getServerConnectionInfo(Channel channel) {
    return String.format("[%s -> %s]", channel.localAddress(), channel.remoteAddress());
  }

  private static class AllocatorHolder {
    private static final AbstractByteBufAllocator INSTANCE = createUnpooledByteBufAllocator(true);
  }

  public static AbstractByteBufAllocator getNettyBufferAllocator() {
    return AllocatorHolder.INSTANCE;
  }

  public static UnpooledByteBufAllocator createUnpooledByteBufAllocator(boolean preferDirect) {
    return new UnpooledByteBufAllocator(preferDirect);
  }

  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY_IN_BYTES;
  }
}
