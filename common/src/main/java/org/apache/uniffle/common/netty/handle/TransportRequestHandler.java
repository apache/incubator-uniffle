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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.protocol.RequestMessage;

public class TransportRequestHandler extends MessageHandler<RequestMessage> {

  private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** Client on the same channel allowing us to talk back to the requester. */
  private final TransportClient reverseClient;

  /** Handles all RPC messages. */
  private final BaseMessageHandler msgHandler;

  public TransportRequestHandler(TransportClient reverseClient, BaseMessageHandler msgHandler) {
    this.reverseClient = reverseClient;
    this.msgHandler = msgHandler;
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    msgHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelActive() {
    logger.debug("channelActive: {}", reverseClient.getSocketAddress());
  }

  @Override
  public void channelInactive() {
    logger.debug("channelInactive: {}", reverseClient.getSocketAddress());
  }

  @Override
  public void handle(RequestMessage request) {
    msgHandler.receive(reverseClient, request);
  }
}
