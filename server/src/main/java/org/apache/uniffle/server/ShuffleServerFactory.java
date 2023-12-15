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

import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.server.netty.StreamServer;
import org.apache.uniffle.server.rpc.HybridRpcServer;

public class ShuffleServerFactory {

  private final ShuffleServer shuffleServer;
  private final ShuffleServerConf conf;

  public ShuffleServerFactory(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.conf = shuffleServer.getShuffleServerConf();
  }

  public ServerInterface getServer() {
    ServerType type = conf.get(ShuffleServerConf.RPC_SERVER_TYPE);
    if (type == ServerType.NETTY) {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }

    GrpcServer grpcServer = null;
    StreamServer nettyServer = null;
    if (type.withGrpc()) {
      grpcServer =
          GrpcServer.Builder.newBuilder()
              .conf(conf)
              .grpcMetrics(shuffleServer.getGrpcMetrics())
              .addService(new ShuffleServerGrpcService(shuffleServer))
              // todo: Add ServerInterceptor for authentication
              .addService(new ShuffleServerInternalGrpcService(shuffleServer))
              .build();
      if (type == ServerType.GRPC) {
        return grpcServer;
      }
    }

    if (type.withNetty()) {
      nettyServer = new StreamServer(shuffleServer);
      if (type == ServerType.NETTY) {
        return nettyServer;
      }
    }

    return new HybridRpcServer(grpcServer, nettyServer);
  }

  public ShuffleServer getShuffleServer() {
    return shuffleServer;
  }

  public ShuffleServerConf getConf() {
    return conf;
  }
}
