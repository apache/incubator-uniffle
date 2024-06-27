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

package org.apache.uniffle.shuffle.manager;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.ServerType;

public class ShuffleManagerServerFactory {
  private final RssShuffleManagerInterface shuffleManager;
  private final RssBaseConf conf;

  public ShuffleManagerServerFactory(RssShuffleManagerInterface shuffleManager, RssConf conf) {
    this.shuffleManager = shuffleManager;
    this.conf = new RssBaseConf();
    this.conf.addAll(conf);
  }

  public ShuffleManagerGrpcService getService() {
    return new ShuffleManagerGrpcService(shuffleManager);
  }

  public GrpcServer getServer() {
    return getServer(null);
  }

  public GrpcServer getServer(ShuffleManagerGrpcService service) {
    ServerType type = conf.get(RssBaseConf.RPC_SERVER_TYPE);
    if (type == ServerType.GRPC || type == ServerType.GRPC_NETTY) {
      if (service == null) {
        service = new ShuffleManagerGrpcService(shuffleManager);
      }
      return GrpcServer.Builder.newBuilder()
          .conf(conf)
          .grpcMetrics(GRPCMetrics.getEmptyGRPCMetrics(conf))
          .addService(service)
          .build();
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }

  public RssBaseConf getConf() {
    return conf;
  }
}
