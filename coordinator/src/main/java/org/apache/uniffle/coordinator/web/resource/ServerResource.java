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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.request.CancelDecommissionRequest;
import org.apache.uniffle.coordinator.web.request.DecommissionRequest;

@Produces({MediaType.APPLICATION_JSON})
public class ServerResource extends BaseResource {
  @Context protected ServletContext servletContext;

  @GET
  @Path("/{id}")
  public Response<ServerNode> node(@PathParam("id") String id) {
    return execute(() -> getClusterManager().getServerNodeById(id));
  }

  @GET
  @Path("/nodes")
  public Response<List<ServerNode>> nodes() {
    return nodes(null);
  }

  @GET
  @Path("/nodes/{status}")
  public Response<List<ServerNode>> nodes(
      @PathParam("status") String status) {
    ClusterManager clusterManager = getClusterManager();
    List<ServerNode> serverList = null;
    if (ServerStatus.UNHEALTHY.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getUnhealthyServerList();
    } else if (ServerStatus.LOST.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getLostServerList();
    } else if ("excluded".equalsIgnoreCase(status)) {
      serverList = new ArrayList<>();
      Set<String> excludeNodes = clusterManager.getExcludeNodes();
      for (String excludeNode : excludeNodes) {
        serverList.add(new ServerNode("",excludeNode, 0, 0,
            0,0,0, Sets.newConcurrentHashSet()));
      }
    } else if (ServerStatus.DECOMMISSIONING.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getDecommissioningServerList();
    } else if (ServerStatus.DECOMMISSIONED.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getDecommissionedServerList();
    } else {
      serverList = clusterManager.getServerList(Collections.emptySet());
    }
    serverList.sort(Comparator.comparing(ServerNode::getId));
    return Response.success(serverList);
  }

  @POST
  @Path("/cancelDecommission")
  public Response<Object> cancelDecommission(CancelDecommissionRequest params) {
    return execute(
        () -> {
          assert CollectionUtils.isNotEmpty(params.getServerIds())
              : "Parameter[serverIds] should not be null!";
          params.getServerIds().forEach(getClusterManager()::cancelDecommission);
          return null;
        });
  }

  @POST
  @Path("/{id}/cancelDecommission")
  public Response<Object> cancelDecommission(@PathParam("id") String serverId) {
    return execute(
        () -> {
          getClusterManager().cancelDecommission(serverId);
          return null;
        });
  }

  @POST
  @Path("/decommission")
  public Response<Object> decommission(DecommissionRequest params) {
    return execute(
        () -> {
          assert CollectionUtils.isNotEmpty(params.getServerIds())
              : "Parameter[serverIds] should not be null!";
          params.getServerIds().forEach(getClusterManager()::decommission);
          return null;
        });
  }

  @POST
  @Path("/{id}/decommission")
  @Produces({MediaType.APPLICATION_JSON})
  public Response<Object> decommission(@PathParam("id") String serverId) {
    return execute(
        () -> {
          getClusterManager().decommission(serverId);
          return null;
        });
  }

  @GET
  @Path("/nodestatustotal")
  public Response<Map<String, Integer>> getNodeStatusTotal() {
    return execute(() -> {
      ClusterManager clusterManager = getClusterManager();
      Map<String, Integer> stringIntegerHash = new HashMap();
      stringIntegerHash.put("activeNode", clusterManager.getServerList(Sets.newConcurrentHashSet()).size());
      stringIntegerHash.put("decommissioningNode", clusterManager.getDecommissioningServerList().size());
      stringIntegerHash.put("decommissionedNode", clusterManager.getDecommissionedServerList().size());
      stringIntegerHash.put("lostNode", clusterManager.getLostServerList().size());
      stringIntegerHash.put("unhealthyNode", clusterManager.getUnhealthyServerList().size());
      stringIntegerHash.put("excludesNode", clusterManager.getExcludeNodes().size());
      return stringIntegerHash;
    });
  }

  private ClusterManager getClusterManager() {
    return (ClusterManager) servletContext.getAttribute(ClusterManager.class.getCanonicalName());
  }
}
