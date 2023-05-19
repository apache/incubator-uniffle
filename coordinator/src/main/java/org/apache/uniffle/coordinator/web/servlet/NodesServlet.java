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

package org.apache.uniffle.coordinator.web.servlet;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.web.Response;


public class NodesServlet extends BaseServlet {
  private final CoordinatorServer coordinator;

  public NodesServlet(CoordinatorServer coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected <T> Response<T> handleGet(HttpServletRequest req, HttpServletResponse resp) {
    List<ServerNode> serverList = coordinator.getClusterManager().getServerList(Collections.EMPTY_SET);
    String id = req.getParameter("id");
    String status = req.getParameter("status");
    serverList = serverList.stream().filter((server) -> {
      if (id != null && !id.equals(server.getId())) {
        return false;
      }
      if (status != null && !server.getStatus().toString().equals(status)) {
        return false;
      }
      return true;
    }).collect(Collectors.toList());
    Collections.sort(serverList, Comparator.comparing(ServerNode::getId));
    return (Response<T>) Response.success(serverList);
  }
}
