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

package org.apache.uniffle.common.netty.client;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.handle.TransportResponseHandler;
import org.apache.uniffle.common.netty.protocol.MessageEncoder;

public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private TransportConf transportConf;

  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;

  public TransportContext(TransportConf transportConf) {
    this.transportConf = transportConf;
  }

  public TransportClientFactory createClientFactory() {
    return new TransportClientFactory(this);
  }

  public TransportResponseHandler initializePipeline(
      SocketChannel channel, ChannelInboundHandlerAdapter decoder) {
    try {
      TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
      channel
          .pipeline()
          .addLast("encoder", ENCODER) // out
          .addLast("decoder", decoder) // in
          .addLast(
              "idleStateHandler", new IdleStateHandler(0, 0, transportConf.connectionTimeoutMs() / 1000))
          .addLast("responseHandler", responseHandler);
      return responseHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  public TransportConf getConf() {
    return transportConf;
  }
}
