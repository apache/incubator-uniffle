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
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;

public class MonitoringServerCall<R, S> extends ForwardingServerCall.SimpleForwardingServerCall<R, S> {

  private final String methodName;
  private final GRPCMetrics grpcMetrics;

  protected MonitoringServerCall(
      ServerCall<R, S> delegate, String methodName, GRPCMetrics grpcMetrics) {
    super(delegate);
    this.methodName = methodName;
    this.grpcMetrics = grpcMetrics;
  }

  @Override
  public void close(Status status, Metadata responseHeaders) {
    try {
      super.close(status, responseHeaders);
    } finally {
      grpcMetrics.decCounter(methodName);
    }
  }
}
