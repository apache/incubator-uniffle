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

package org.apache.uniffle.server.netty.decoder;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.MessageConstants;
import org.apache.uniffle.server.netty.message.GetLocalShuffleDataMessage;
import org.apache.uniffle.server.netty.util.NettyUtils;

public class GetLocalShuffleDataDecoder extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataMessageDecoder.class);

  private DecoderState state = DecoderState.READ_MAGIC_BYTE;
  private int requiredBytes = 0;
  private GetLocalShuffleDataMessage getLocalShuffleDataMessage = new GetLocalShuffleDataMessage();

  public GetLocalShuffleDataDecoder() {
    super();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (in.readableBytes() == 0) {
      return;
    }

    switch (state) {
      case READ_MAGIC_BYTE:
        if (in.readableBytes() < Byte.BYTES) {
          return;
        }
        byte magicByte = in.readByte();
        switch (magicByte) {
          case MessageConstants.GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE:
            // start to process data upload
            state = DecoderState.READ_TASK_APPID_LEN;
            return;
          default:
            String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
            LOG.warn(
                "Invalid magic byte {} from client {}",
                magicByte, clientInfo);
            ctx.close();
            LOG.debug("Closed connection to client {}", clientInfo);
            return;
        }
      case READ_TASK_APPID_LEN:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        // read length of appId
        requiredBytes = in.readInt();
        if (requiredBytes < 0) {
          String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
          LOG.warn(
              "Invalid length of applicationId {} from client {}",
              requiredBytes, clientInfo);
          ctx.close();
          LOG.debug("Closed connection to client {}", clientInfo);
          return;
        }
        state = DecoderState.READ_TASK_FETCH_DATA_INFO;
        return;
      case READ_TASK_FETCH_DATA_INFO:
        // appId + shuffleId + partitionId + partitionNumPerRange + PartitionNum + ReadBufferSize
        if (in.readableBytes() < requiredBytes + 6 * Integer.BYTES + Long.BYTES) {
          return;
        }
        getLocalShuffleDataMessage.readMessageInfo(in, requiredBytes);
        out.add(getLocalShuffleDataMessage);
        return;
      default:
        throw new RuntimeException(String.format(
            "Should not get incoming data in state %s, client %s",
            state, NettyUtils.getServerConnectionInfo(ctx)));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);

    ctx.close();
  }

}
