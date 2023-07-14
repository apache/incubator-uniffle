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

package org.apache.uniffle.coordinator;

import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.rpc.ServerType;

public class CoordinatorFactory {

  private final CoordinatorServer coordinatorServer;
  private final CoordinatorConf conf;

  public CoordinatorFactory(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
    this.conf = coordinatorServer.getCoordinatorConf();
  }

  public ServerInterface getServer() {
    ServerType type = conf.get(CoordinatorConf.RPC_SERVER_TYPE);
    // Coordinator currently only has grpc support. However, we should support create grpc server
    // even if the server type is GRPC_NETTY. Otherwise, we cannot use a unified configuration
    // to start both coordinator and shuffle server
    if (type == ServerType.GRPC || type == ServerType.GRPC_NETTY) {
      return GrpcServer.Builder.newBuilder()
          .conf(conf)
          .grpcMetrics(coordinatorServer.getGrpcMetrics())
          .addService(new CoordinatorGrpcService(coordinatorServer))
          .build();
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }
}
