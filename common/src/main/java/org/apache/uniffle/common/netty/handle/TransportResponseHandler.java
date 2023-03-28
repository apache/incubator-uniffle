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

package org.apache.uniffle.common.netty.handle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.client.RpcResponseCallback;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.util.NettyUtils;


public class TransportResponseHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private Map<Long, RpcResponseCallback> outstandingRpcRequests;
  private Channel channel;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingRpcRequests = new ConcurrentHashMap<>();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof RpcResponse) {
      RpcResponse responseMessage = (RpcResponse) msg;
      RpcResponseCallback listener = outstandingRpcRequests.get(responseMessage.getRequestId());
      if (listener == null) {
        logger.warn("Ignoring response from {} since it is not outstanding",
            NettyUtils.getRemoteAddress(channel));
      } else {
        listener.onSuccess(responseMessage);
      }
    } else {
      throw new RssException("receive unexpected message!");
    }
    super.channelRead(ctx, msg);
  }

  public void addResponseCallback(long requestId, RpcResponseCallback callback) {
    outstandingRpcRequests.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcRequests.remove(requestId);
  }


}
