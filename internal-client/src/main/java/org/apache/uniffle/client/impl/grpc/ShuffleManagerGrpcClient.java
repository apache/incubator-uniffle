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

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureResponse;
import org.apache.uniffle.proto.ShuffleManagerGrpc;

public class ShuffleManagerGrpcClient extends GrpcClient implements ShuffleManagerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcClient.class);
  private static final long RPC_TIMEOUT_DEFAULT_MS = 60000;
  private long rpcTimeout = RPC_TIMEOUT_DEFAULT_MS;
  private ShuffleManagerGrpc.ShuffleManagerBlockingStub blockingStub;

  public ShuffleManagerGrpcClient(String host, int port) {
    this(host, port, 3);
  }

  public ShuffleManagerGrpcClient(String host, int port, int maxRetryAttempts) {
    this(host, port, maxRetryAttempts, true);
  }

  public ShuffleManagerGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
    blockingStub = ShuffleManagerGrpc.newBlockingStub(channel);
  }

  public ShuffleManagerGrpc.ShuffleManagerBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
  }

  public String getDesc() {
    return "Shuffle manager grpc client ref " + host + ":" + port;
  }

  @Override
  public RssReportShuffleFetchFailureResponse reportShuffleFetchFailure(RssReportShuffleFetchFailureRequest request) {
    ReportShuffleFetchFailureRequest protoRequest = request.toProto();
    try {
      ReportShuffleFetchFailureResponse response = getBlockingStub().reportShuffleFetchFailure(protoRequest);
      return RssReportShuffleFetchFailureResponse.fromProto(response);
    } catch (Exception e) {
      String msg = "Report shuffle fetch failure to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }
}
