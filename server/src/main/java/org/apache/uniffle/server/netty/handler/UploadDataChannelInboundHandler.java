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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.network.protocol.UploadDataResponse;
import org.apache.uniffle.server.netty.util.HandlerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.util.MessageConstants;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.netty.message.UploadDataMessage;
import org.apache.uniffle.server.netty.util.NettyUtils;

public class UploadDataChannelInboundHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataChannelInboundHandler.class);

  private final long idleTimeoutMillis;

  private String connectionInfo = "";

  private ShuffleServer shuffleServer;
  private IdleCheck idleCheck;

  public UploadDataChannelInboundHandler(
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

  public void processChannelActive(final ChannelHandlerContext ctx) {
    // colinmjj: add metrics for connection
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

    idleCheck = new IdleCheck(ctx, idleTimeoutMillis);
    schedule(ctx, idleCheck, idleTimeoutMillis);
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
      if (idleCheck != null) {
        idleCheck.updateLastReadTime();
      }
      StatusCode ret = StatusCode.SUCCESS;
      String responseMessage = "OK";

      if (msg instanceof UploadDataMessage) {
        UploadDataMessage uploadDataMessage = (UploadDataMessage) msg;
        long requestId = uploadDataMessage.getRequestId();
        String appId = uploadDataMessage.getAppId();
        long requireBufferId = uploadDataMessage.getRequireBufferId();
        int requireSize = shuffleServer
                              .getShuffleTaskManager().getRequireBufferSize(requireBufferId);
        int shuffleId = uploadDataMessage.getShuffleId();
        Map<Integer, List<ShufflePartitionedBlock>> shuffleData = uploadDataMessage.getShuffleData();
        final AtomicInteger blockNum = new AtomicInteger();
        final AtomicLong size = new AtomicLong();
        shuffleData.values().stream().flatMap(Collection::stream).forEach(shufflePartitionedBlock -> {
          blockNum.incrementAndGet();
          size.addAndGet(shufflePartitionedBlock.getSize());
        });
//        LOG.info("SSSSSS: UploadData shuffleId=" + shuffleId + " requireSize=" + requireSize + " blockNum=" + blockNum + " size=" + size);
        if (!shuffleData.isEmpty()) {
          boolean isPreAllocated =
              shuffleServer.getShuffleTaskManager().isPreAllocated(requireBufferId);
          if (!isPreAllocated) {
            String errorMsg = "Can't find requireBufferId[" + requireBufferId + "] for appId[" + appId
                                  + "], shuffleId[" + shuffleId + "]";
            LOG.warn(errorMsg);
            ret = StatusCode.INTERNAL_ERROR;
            responseMessage = errorMsg;
            UploadDataResponse response = new UploadDataResponse(requestId, ret, responseMessage);
            ChannelFuture channelFuture = HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
//            channelFuture.addListener(ChannelFutureListener.CLOSE);
            return;
          }
          final long start = System.currentTimeMillis();
          List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(shuffleData);
          for (ShufflePartitionedData spd : shufflePartitionedData) {
            String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + shuffleId
                                         + "], partitionId[" + spd.getPartitionId() + "]";
            try {
//              for(ShufflePartitionedBlock block : spd.getBlockList()) {
//                LOG.info("UploadDataChannelInboundHandler cacheShuffleData partitionId=" + spd.getPartitionId() + " blockId=" +  block.getBlockId() + ", size=" + block.getSize());
//              }
              ret = shuffleServer
                        .getShuffleTaskManager()
                        .cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
              if (ret != StatusCode.SUCCESS) {
                String errorMsg = "Error happened when shuffleEngine.write for "
                                      + shuffleDataInfo + ", statusCode=" + ret;
                LOG.error(errorMsg);
                responseMessage = errorMsg;
                break;
              } else {
                // remove require bufferId, the memory should be updated already
                shuffleServer
                    .getShuffleTaskManager().removeRequireBufferId(requireBufferId);
                shuffleServer.getShuffleTaskManager().updateCachedBlockIds(
                    appId, shuffleId, spd.getBlockList());
              }
            } catch (Exception e) {
              String errorMsg = "Error happened when shuffleEngine.write for "
                                    + shuffleDataInfo + ": " + e.getMessage();
              ret = StatusCode.INTERNAL_ERROR;
              responseMessage = errorMsg;
              LOG.error(errorMsg);
              break;
            }
          }
          LOG.debug("Cache Shuffle Data for appId[" + appId + "], shuffleId[" + shuffleId
                        + "], cost " + (System.currentTimeMillis() - start)
                        + " ms with " + shufflePartitionedData.size() + " blocks and " + requireSize + " bytes");
        } else {
          ret = StatusCode.INTERNAL_ERROR;
          responseMessage = "No data in request";
        }
        UploadDataResponse response = new UploadDataResponse(requestId, ret, responseMessage);
        ChannelFuture channelFuture = HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
//        channelFuture.addListener(ChannelFutureListener.CLOSE);
      } else {
        throw new RuntimeException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  private List<ShufflePartitionedData> toPartitionedData(Map<Integer, List<ShufflePartitionedBlock>> partitionToBlocks) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();
    for(Map.Entry<Integer, List<ShufflePartitionedBlock>> entry : partitionToBlocks.entrySet()){
      int partitionId = entry.getKey();
      List<ShufflePartitionedBlock> blocks = entry.getValue();
      ret.add(new ShufflePartitionedData(partitionId, toPartitionedBlock(blocks)));
    }

    return ret;
  }

  private ShufflePartitionedBlock[] toPartitionedBlock(List<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return new ShufflePartitionedBlock[]{};
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    int i = 0;
    for (ShufflePartitionedBlock block : blocks) {
      ret[i] = new ShufflePartitionedBlock(
          block.getLength(),
          block.getUncompressLength(),
          block.getCrc(),
          block.getBlockId(),
          block.getTaskAttemptId(),
          block.getData());
      i++;
    }
    return ret;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);
    ctx.close();
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
