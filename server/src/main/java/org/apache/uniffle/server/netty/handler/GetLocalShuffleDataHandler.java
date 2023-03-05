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

package org.apache.uniffle.server.netty.handler;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.MessageConstants;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.netty.message.GetLocalShuffleDataMessage;
import org.apache.uniffle.server.netty.util.NettyUtils;

public class GetLocalShuffleDataHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataChannelInboundHandler.class);

  private final long idleTimeoutMillis;

  private String connectionInfo = "";

  private ShuffleServer shuffleServer;
  private IdleCheck idleCheck;

  public GetLocalShuffleDataHandler(
      ShuffleServer shuffleServer) {
    this.idleTimeoutMillis = shuffleServer.getShuffleServerConf().getLong(
        ShuffleServerConf.SERVER_NETTY_HANDLER_IDLE_TIMEOUT);
    this.shuffleServer = shuffleServer;
  }

  private static void schedule(ChannelHandlerContext ctx, Runnable task, long delayMillis) {
    ctx.executor().schedule(task, delayMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    processChannelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    LOG.debug("Channel inactive: {}", connectionInfo);

    if (idleCheck != null) {
      idleCheck.cancel();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      ByteBuf resultBuf = ctx.alloc().buffer(1);
      if (idleCheck != null) {
        idleCheck.updateLastReadTime();
      }

      if (msg instanceof GetLocalShuffleDataMessage) {
        GetLocalShuffleDataMessage getLocalShuffleDataMessage = (GetLocalShuffleDataMessage) msg;
        String appId = getLocalShuffleDataMessage.getAppId();
        int shuffleId = getLocalShuffleDataMessage.getShuffleId();
        int partitionId = getLocalShuffleDataMessage.getPartitionId();
        int partitionNumPerRange = getLocalShuffleDataMessage.getPartitionNumPerRange();
        int partitionNum = getLocalShuffleDataMessage.getPartitionNum();
        int length = getLocalShuffleDataMessage.getLength();
        long offset = getLocalShuffleDataMessage.getOffset();
        String storageType = shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE);
        String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId["
                                 + partitionId + "]";
        try {
          if (shuffleServer.getShuffleBufferManager().requireReadMemoryWithRetry(length)) {
            try {
              long start = System.currentTimeMillis();
              ShuffleDataResult sdr = shuffleServer.getShuffleTaskManager().getShuffleData(
                  appId, shuffleId, partitionId, partitionNumPerRange, partitionNum,
                  storageType, offset, length);
              LOG.info("GetLocalShuffleData with netty cost {} ms for {} with {}",
                  (System.currentTimeMillis() - start), sdr.getData().length, requestInfo);
              doResponse(ctx, sdr);
            } finally {
              shuffleServer.getShuffleBufferManager().releaseReadMemory(length);
            }
          } else {
            String errorMsg = "Can't require memory to get shuffle data";
            LOG.error(errorMsg + " for " + requestInfo);
            throw new RuntimeException(errorMsg);
          }
        } catch (Exception e) {
          LOG.warn(
              "Error happened when get shuffle data for " + requestInfo + " from " + connectionInfo, e);
          resultBuf.writeByte(MessageConstants.RESPONSE_STATUS_ERROR);
          ctx.writeAndFlush(resultBuf).addListener(ChannelFutureListener.CLOSE);
        }
      } else {
        throw new RuntimeException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);
    ctx.close();
  }

  private void doResponse(ChannelHandlerContext ctx, ShuffleDataResult sdr) {
    ByteBuf resultBuf = ctx.alloc().buffer(1);
    resultBuf.writeByte(MessageConstants.RESPONSE_STATUS_OK);
    ctx.write(resultBuf);
    ctx.writeAndFlush(Unpooled.wrappedBuffer(sdr.getData()))
        .addListener(ChannelFutureListener.CLOSE);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    // colinmjj: add metrics for connection
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

    idleCheck = new IdleCheck(ctx, idleTimeoutMillis);
    schedule(ctx, idleCheck, idleTimeoutMillis);
  }

  private static class IdleCheck implements Runnable {

    private final ChannelHandlerContext ctx;
    private final long idleTimeoutMillis;

    private volatile long lastReadTime = System.currentTimeMillis();
    private volatile boolean canceled = false;

    IdleCheck(ChannelHandlerContext ctx, long idleTimeoutMillis) {
      this.ctx = ctx;
      this.idleTimeoutMillis = idleTimeoutMillis;
    }

    @Override
    public void run() {
      try {
        if (canceled) {
          return;
        }

        if (!ctx.channel().isOpen()) {
          return;
        }

        checkIdle(ctx);
      } catch (Throwable ex) {
        LOG.warn(String.format("Failed to run idle check, %s",
            NettyUtils.getServerConnectionInfo(ctx)), ex);
      }
    }

    public void updateLastReadTime() {
      lastReadTime = System.currentTimeMillis();
    }

    public void cancel() {
      canceled = true;
    }

    private void checkIdle(ChannelHandlerContext ctx) {
      if (System.currentTimeMillis() - lastReadTime >= idleTimeoutMillis) {
        // colinmjj: add metrics
        LOG.info("Closing idle connection {}", NettyUtils.getServerConnectionInfo(ctx));
        ctx.close();
        return;
      }

      schedule(ctx, this, idleTimeoutMillis);
    }
  }
}
