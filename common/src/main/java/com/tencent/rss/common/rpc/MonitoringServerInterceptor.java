/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.common.rpc;

import com.tencent.rss.common.metrics.GRPCMetrics;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class MonitoringServerInterceptor implements ServerInterceptor {

  private final GRPCMetrics grpcMetrics;

  public MonitoringServerInterceptor(GRPCMetrics grpcMetrics) {
    this.grpcMetrics = grpcMetrics;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    MethodDescriptor<ReqT, RespT> methodDescriptor = serverCall.getMethodDescriptor();
    String methodName = methodDescriptor.getBareMethodName();
    // a call is coming
    grpcMetrics.incCounter(methodName);
    MonitoringServerCall<ReqT, RespT> monitoringServerCall =
        new MonitoringServerCall<>(serverCall, methodName, grpcMetrics);
    return new MonitoringServerCallListener<>(
        serverCallHandler.startCall(monitoringServerCall, metadata));
  }
}
