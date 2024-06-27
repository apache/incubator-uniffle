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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.client.RpcResponseCallback;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.NettyUtils;

public class TransportResponseHandler extends MessageHandler<RpcResponse> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private Map<Long, RpcResponseCallback> outstandingRpcRequests;
  private Channel channel;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingRpcRequests = JavaUtils.newConcurrentMap();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  public void addResponseCallback(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    if (outstandingRpcRequests.containsKey(requestId)) {
      logger.warn("[addRpcRequest] requestId {} already exists!", requestId);
    }
    outstandingRpcRequests.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcRequests.remove(requestId);
  }

  @Override
  public void handle(RpcResponse message) throws Exception {
    RpcResponseCallback listener = outstandingRpcRequests.get(message.getRequestId());
    if (listener == null) {
      logger.error(
          "Ignoring response from {} since it is not outstanding, {} {}",
          NettyUtils.getRemoteAddress(channel),
          message.type(),
          message.getRequestId());
    } else {
      listener.onSuccess(message);
    }
  }

  @Override
  public void channelActive() {}

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error(
          "Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(),
          remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error(
          "Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(),
          remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  public int numOutstandingRequests() {
    return outstandingRpcRequests.size();
  }

  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcRequests.entrySet()) {
      try {
        entry.getValue().onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }

    outstandingRpcRequests.clear();
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }
}
