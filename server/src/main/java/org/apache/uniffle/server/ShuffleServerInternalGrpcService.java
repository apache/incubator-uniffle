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

package org.apache.uniffle.server;

import io.grpc.stub.StreamObserver;

import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleServerInternalGrpc.ShuffleServerInternalImplBase;

public class ShuffleServerInternalGrpcService extends ShuffleServerInternalImplBase {
  private final ShuffleServer shuffleServer;

  public ShuffleServerInternalGrpcService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  @Override
  public void decommission(
      RssProtos.DecommissionRequest request,
      StreamObserver<RssProtos.DecommissionResponse> responseObserver) {
    RssProtos.DecommissionResponse response;
    try {
      shuffleServer.decommission();
      response =
          RssProtos.DecommissionResponse.newBuilder()
              .setStatus(StatusCode.SUCCESS.toProto())
              .build();
    } catch (Exception e) {
      StatusCode statusCode = StatusCode.INTERNAL_ERROR;
      if (e instanceof InvalidRequestException) {
        statusCode = StatusCode.INVALID_REQUEST;
      }
      response =
          RssProtos.DecommissionResponse.newBuilder()
              .setStatus(statusCode.toProto())
              .setRetMsg(e.getMessage())
              .build();
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void cancelDecommission(
      RssProtos.CancelDecommissionRequest request,
      StreamObserver<RssProtos.CancelDecommissionResponse> responseObserver) {
    RssProtos.CancelDecommissionResponse response;
    try {
      shuffleServer.cancelDecommission();
      response =
          RssProtos.CancelDecommissionResponse.newBuilder()
              .setStatus(StatusCode.SUCCESS.toProto())
              .build();
    } catch (Exception e) {
      StatusCode statusCode = StatusCode.INTERNAL_ERROR;
      if (e instanceof InvalidRequestException) {
        statusCode = StatusCode.INVALID_REQUEST;
      }
      response =
          RssProtos.CancelDecommissionResponse.newBuilder()
              .setStatus(statusCode.toProto())
              .setRetMsg(e.getMessage())
              .build();
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
