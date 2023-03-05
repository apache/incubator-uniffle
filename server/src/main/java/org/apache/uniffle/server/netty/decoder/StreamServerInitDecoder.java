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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.uniffle.server.netty.handler.UploadDataChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.MessageConstants;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.netty.handler.GetInMemoryShuffleDataHandler;
import org.apache.uniffle.server.netty.handler.GetLocalShuffleDataHandler;
import org.apache.uniffle.server.netty.util.NettyUtils;

public class StreamServerInitDecoder extends ByteToMessageDecoder {

  private static final Logger logger = LoggerFactory.getLogger(StreamServerInitDecoder.class);

  private ShuffleServer shuffleServer;

  public StreamServerInitDecoder(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  private void addDecoder(ChannelHandlerContext ctx, byte type) {
    final String decoderName = "decoder";
    final String handlerName = "handler";
    ByteToMessageDecoder newDecoder;
    ChannelInboundHandlerAdapter newHandler;

    if (type == MessageConstants.GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE) {
      newDecoder = new GetLocalShuffleDataDecoder();
      GetLocalShuffleDataHandler channelInboundHandler =
          new GetLocalShuffleDataHandler(shuffleServer);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else if (type == MessageConstants.GET_IN_MEMORY_SHUFFLE_DATA_MAGIC_BYTE) {
      newDecoder = new GetInMemoryShuffleDataDecoder();
      GetInMemoryShuffleDataHandler channelInboundHandler =
          new GetInMemoryShuffleDataHandler(shuffleServer);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else if (type == MessageConstants.UPLOAD_DATA_MAGIC_BYTE) {
      ByteBuf shuffleDataBuffer = ctx.alloc().directBuffer(MessageConstants.DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE);
      newDecoder = new UploadDataMessageDecoder(shuffleDataBuffer);
      UploadDataChannelInboundHandler channelInboundHandler =
          new UploadDataChannelInboundHandler(shuffleServer);
      channelInboundHandler.processChannelActive(ctx);
      newHandler = channelInboundHandler;
    } else {
      String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
      logger.error(String.format(
          "Invalid magic code %d for link type %s from client %s",
          type, type, clientInfo));
      ctx.close();
      logger.info(String.format("Closed connection to client %s", clientInfo));
      return;
    }
    logger.debug(
        String.format("Using protocol for client %s", NettyUtils.getServerConnectionInfo(ctx)));
    ctx.pipeline().replace(this, decoderName, newDecoder);
    ctx.pipeline().addAfter(decoderName, handlerName, newHandler);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx,
      ByteBuf in,
      List<Object> out) throws Exception {
    if (in.readableBytes() < Byte.BYTES) {
      return;
    }
    in.markReaderIndex();
    byte magicByte = in.readByte();
    in.resetReaderIndex();  // rewind so that the newly added decoder can re-read it

    addDecoder(ctx, magicByte);
  }

  // Newly added handlers will then re-process the message.
}
