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

import java.util.ArrayList;
import java.util.List;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;

public class ReceivingFailureServer {
  private String serverId;
  private StatusCode statusCode;

  private ReceivingFailureServer() {
    // ignore
  }

  public ReceivingFailureServer(String serverId, StatusCode statusCode) {
    this.serverId = serverId;
    this.statusCode = statusCode;
  }

  public String getServerId() {
    return serverId;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  @Override
  public String toString() {
    return "ReceivingFailureServer{"
        + "serverId='"
        + serverId
        + '\''
        + ", statusCode="
        + statusCode
        + '}';
  }

  public static List<ReceivingFailureServer> fromProto(RssProtos.ReceivingFailureServers proto) {
    List<ReceivingFailureServer> servers = new ArrayList<>();
    for (RssProtos.ReceivingFailureServer protoServer : proto.getServerList()) {
      ReceivingFailureServer server = new ReceivingFailureServer();
      server.serverId = protoServer.getServerId();
      server.statusCode = StatusCode.fromProto(protoServer.getStatusCode());
      servers.add(server);
    }
    return servers;
  }

  public static RssProtos.ReceivingFailureServers toProto(List<ReceivingFailureServer> servers) {
    List<RssProtos.ReceivingFailureServer> protoServers = new ArrayList<>();
    for (ReceivingFailureServer server : servers) {
      protoServers.add(
          RssProtos.ReceivingFailureServer.newBuilder()
              .setServerId(server.serverId)
              .setStatusCode(server.statusCode.toProto())
              .build());
    }
    return RssProtos.ReceivingFailureServers.newBuilder().addAllServer(protoServers).build();
  }

  public static RssProtos.ReceivingFailureServer toProto(ReceivingFailureServer server) {
    return RssProtos.ReceivingFailureServer.newBuilder()
        .setServerId(server.serverId)
        .setStatusCode(server.statusCode.toProto())
        .build();
  }
}
