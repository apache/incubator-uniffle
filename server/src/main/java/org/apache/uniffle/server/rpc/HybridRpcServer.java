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

package org.apache.uniffle.server.rpc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.server.netty.StreamServer;

/** This class is to hold the multiple rpc service for the rpc mode of GRPC_NETTY */
public class HybridRpcServer implements ServerInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridRpcServer.class);

  private final GrpcServer grpcServer;
  private final StreamServer nettyServer;

  private int grpcPort;
  private int nettyPort;

  public HybridRpcServer(GrpcServer grpcServer, StreamServer nettyServer) {
    this.grpcServer = grpcServer;
    this.nettyServer = nettyServer;
  }

  @Override
  public int start() throws IOException {
    this.grpcPort = grpcServer.start();
    this.nettyPort = nettyServer.start();
    return -1;
  }

  @Override
  public void startOnPort(int port) throws Exception {
    // ignore
  }

  @Override
  public void stop() throws InterruptedException {
    grpcServer.stop();
    nettyServer.stop();
  }

  @Override
  public void blockUntilShutdown() throws InterruptedException {
    // ignore the netty server blocking wait
    grpcServer.blockUntilShutdown();
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public int getNettyPort() {
    return nettyPort;
  }
}
