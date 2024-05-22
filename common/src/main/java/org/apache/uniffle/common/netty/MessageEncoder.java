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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.protocol.Message;
import org.apache.uniffle.common.netty.protocol.MessageWithHeader;
import org.apache.uniffle.common.netty.protocol.RpcResponse;

/**
 * Encoder used by the server side to encode server-to-client responses. This encoder is stateless
 * so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private static final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  public static final MessageEncoder INSTANCE = new MessageEncoder();

  private MessageEncoder() {}

  /**
   * * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    int bodyLength = 0;

    // If the message has a body, take it out to enable zero-copy transfer for the payload.
    if (in.body() != null) {
      try {
        bodyLength = (int) in.body().size();
        body = in.body().convertToNetty();
      } catch (Exception e) {
        in.body().release();
        if (in instanceof RpcResponse) {
          RpcResponse resp = (RpcResponse) in;
          // Re-encode this message as a failure response.
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error(
              String.format("Error processing %s for client %s", in, ctx.channel().remoteAddress()),
              e);
          encode(ctx, resp.createFailureResponse(error), out);
        } else {
          throw e;
        }
        return;
      }
    }

    Message.Type msgType = in.type();
    // message size, message type size, body size, message encoded length
    int headerLength = Integer.BYTES + msgType.encodedLength() + Integer.BYTES + in.encodedLength();
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeInt(in.encodedLength());
    msgType.encode(header);
    header.writeInt(bodyLength);
    in.encode(header);
    assert header.writableBytes() == 0;

    if (body != null) {
      // We transfer ownership of the reference on in.body() to MessageWithHeader.
      // This reference will be freed when MessageWithHeader.deallocate() is called.
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      out.add(header);
    }
  }
}
