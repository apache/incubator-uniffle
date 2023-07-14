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

package org.apache.uniffle.common;

import java.io.Serializable;

public class ShuffleServerInfo implements Serializable {

  private String id;

  private String host;

  private int grpcPort;

  private int nettyPort = -1;

  // Only for test
  public ShuffleServerInfo(String host, int port) {
    this.id = host + "-" + port;
    this.host = host;
    this.grpcPort = port;
  }

  public ShuffleServerInfo(String id, String host, int port) {
    this.id = id;
    this.host = host;
    this.grpcPort = port;
  }

  public ShuffleServerInfo(String id, String host, int grpcPort, int nettyPort) {
    this.id = id;
    this.host = host;
    this.grpcPort = grpcPort;
    this.nettyPort = nettyPort;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public int getNettyPort() {
    return nettyPort;
  }

  @Override
  public int hashCode() {
    // By default id = host + "-" + grpc port, if netty port is greater than 0,
    // id = host + "-" + grpc port + "-" + netty port
    // so it is enough to calculate hashCode with id.
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleServerInfo) {
      return id.equals(((ShuffleServerInfo) obj).getId())
          && host.equals(((ShuffleServerInfo) obj).getHost())
          && grpcPort == ((ShuffleServerInfo) obj).getGrpcPort()
          && nettyPort == ((ShuffleServerInfo) obj).getNettyPort();
    }
    return false;
  }

  @Override
  public String toString() {
    if (nettyPort > 0) {
      return "ShuffleServerInfo{id["
          + id
          + "], host["
          + host
          + "],"
          + " grpc port["
          + grpcPort
          + "], netty port["
          + nettyPort
          + "]}";
    } else {
      return "ShuffleServerInfo{id["
          + id
          + "], host["
          + host
          + "],"
          + " grpc port["
          + grpcPort
          + "]}";
    }
  }
}
