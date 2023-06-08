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

package org.apache.uniffle.coordinator.web.resource;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;

import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.request.CancelDecommissionRequest;
import org.apache.uniffle.coordinator.web.request.DecommissionRequest;

@Path("/server")
@Produces({ MediaType.APPLICATION_JSON })
public class ServerResource {
  @Context
  private HttpServletRequest httpRequest;
  @Context
  protected ServletContext servletContext;

  @GET
  @Path("/nodes")
  public Response<List<ServerNode>> nodes(@QueryParam("id") String id, @QueryParam("status") String status) {
    ClusterManager clusterManager = getClusterManager();
    List<ServerNode> serverList = clusterManager.getServerList(Collections.emptySet());
    serverList = serverList.stream().filter(new Predicate<ServerNode>() {
      @Override
      public boolean test(ServerNode server) {
        if (id != null && !id.equals(server.getId())) {
          return false;
        }
        if (status != null && !server.getStatus().toString().equals(status)) {
          return false;
        }
        return true;
      }
    }).collect(Collectors.toList());
    serverList.sort(Comparator.comparing(new Function<ServerNode, String>() {
      @Override
      public String apply(ServerNode serverNode) {
        return serverNode.getId();
      }
    }));
    return Response.success(serverList);
  }

  @POST
  @Path("/cancelDecommission")
  @Produces({ MediaType.APPLICATION_JSON })
  public Response<Object> cancelDecommission(CancelDecommissionRequest params) {
    if (CollectionUtils.isEmpty(params.getServerIds())) {
      return Response.fail("Parameter[serverIds] should not be null!");
    }
    ClusterManager clusterManager = getClusterManager();
    try {
      for (String serverId : params.getServerIds()) {
        clusterManager.cancelDecommission(serverId);
      }
    } catch (Exception e) {
      return Response.fail(e.getMessage());
    }
    return Response.success(null);
  }

  @POST
  @Path("/decommission")
  @Produces({ MediaType.APPLICATION_JSON })
  public Response<Object> decommission(DecommissionRequest params) {
    if (CollectionUtils.isEmpty(params.getServerIds())) {
      return Response.fail("Parameter[serverIds] should not be null!");
    }
    ClusterManager clusterManager = getClusterManager();
    try {
      for (String serverId : params.getServerIds()) {
        clusterManager.decommission(serverId);
      }
    } catch (Exception e) {
      return Response.fail(e.getMessage());
    }
    return Response.success(null);
  }

  private ClusterManager getClusterManager() {
    return (ClusterManager) servletContext.getAttribute(
        ClusterManager.class.getCanonicalName());
  }
}
