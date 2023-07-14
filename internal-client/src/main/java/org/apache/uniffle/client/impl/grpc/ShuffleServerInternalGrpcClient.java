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

package org.apache.uniffle.client.impl.grpc;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerInternalClient;
import org.apache.uniffle.client.request.RssCancelDecommissionRequest;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.client.response.RssCancelDecommissionResponse;
import org.apache.uniffle.client.response.RssDecommissionResponse;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleServerInternalGrpc;

public class ShuffleServerInternalGrpcClient extends GrpcClient
    implements ShuffleServerInternalClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerInternalGrpcClient.class);
  private static final long RPC_TIMEOUT_DEFAULT_MS = 60000;
  private long rpcTimeout = RPC_TIMEOUT_DEFAULT_MS;
  private ShuffleServerInternalGrpc.ShuffleServerInternalBlockingStub blockingStub;

  public ShuffleServerInternalGrpcClient(String host, int port) {
    this(host, port, 3);
  }

  public ShuffleServerInternalGrpcClient(String host, int port, int maxRetryAttempts) {
    this(host, port, maxRetryAttempts, true);
  }

  public ShuffleServerInternalGrpcClient(
      String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
    // todo Add ClientInterceptor for authentication
    blockingStub = ShuffleServerInternalGrpc.newBlockingStub(channel);
  }

  public ShuffleServerInternalGrpc.ShuffleServerInternalBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public RssDecommissionResponse decommission(RssDecommissionRequest request) {
    RssProtos.DecommissionRequest protoRequest = RssProtos.DecommissionRequest.newBuilder().build();
    RssProtos.DecommissionResponse rpcResponse = getBlockingStub().decommission(protoRequest);
    return new RssDecommissionResponse(
        StatusCode.fromProto(rpcResponse.getStatus()), rpcResponse.getRetMsg());
  }

  @Override
  public RssCancelDecommissionResponse cancelDecommission(
      RssCancelDecommissionRequest rssCancelDecommissionRequest) {
    RssProtos.CancelDecommissionRequest protoRequest =
        RssProtos.CancelDecommissionRequest.newBuilder().build();
    RssProtos.CancelDecommissionResponse rpcResponse =
        getBlockingStub().cancelDecommission(protoRequest);
    return new RssCancelDecommissionResponse(
        StatusCode.fromProto(rpcResponse.getStatus()), rpcResponse.getRetMsg());
  }
}
