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

import com.google.protobuf.BoolValue;
import org.apache.uniffle.client.api.ShuffleServerInternalClient;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.client.response.ResponseStatusCode;
import org.apache.uniffle.client.response.RssDecommissionResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleServerInternalGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ShuffleServerInternalGrpcClient extends GrpcClient implements ShuffleServerInternalClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
  private static final long FAILED_REQUIRE_ID = -1;
  private static final long RPC_TIMEOUT_DEFAULT_MS = 60000;
  private long rpcTimeout = RPC_TIMEOUT_DEFAULT_MS;
  private ShuffleServerInternalGrpc.ShuffleServerInternalBlockingStub blockingStub;

  public ShuffleServerInternalGrpcClient(String host, int port) {
    this(host, port, 3);
  }

  public ShuffleServerInternalGrpcClient(String host, int port, int maxRetryAttempts) {
    this(host, port, maxRetryAttempts, true);
  }

  public ShuffleServerInternalGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
    // todo Add ClientInterceptor for authentication
    blockingStub = ShuffleServerInternalGrpc.newBlockingStub(channel);
  }

  public ShuffleServerInternalGrpc.ShuffleServerInternalBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
  }
  @Override
  public RssDecommissionResponse decommission(RssDecommissionRequest request) {
    RssProtos.DecommissionRequest protoRequest =
        RssProtos.DecommissionRequest.newBuilder().setOn(BoolValue.newBuilder().setValue(request.isOn()).build())
            .build();
    RssProtos.DecommissionResponse rpcResponse =
        blockingStub.withDeadlineAfter(RPC_TIMEOUT_DEFAULT_MS, TimeUnit.MILLISECONDS).decommission(protoRequest);
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    RssDecommissionResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssDecommissionResponse(
            ResponseStatusCode.SUCCESS, rpcResponse.getOn().getValue());
        break;
      default:
        String msg = "Can't " + (request.isOn() ? "enable" : "disable") + " decommission to "
            + host + ":" + port + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }
    return response;
  }
}
