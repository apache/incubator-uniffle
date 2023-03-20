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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.protocol.Message;

public class MessageEncoder extends ChannelOutboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(MessageEncoder.class);

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    // todo: support zero copy
    Message message = (Message) msg;
    int encodeLength = message.encodedLength();
    ByteBuf byteBuf = ctx.alloc().buffer(FrameDecoder.HEADER_SIZE + encodeLength);
    try {
      byteBuf.writeInt(encodeLength);
      byteBuf.writeByte(message.type().id());
      message.encode(byteBuf);
    } catch (Exception e) {
      LOG.error("Unexpected exception during process encode!", e);
      byteBuf.release();
    }
    ctx.writeAndFlush(byteBuf);
  }
}
