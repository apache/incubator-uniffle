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

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;

import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.request.CancelDecommissionRequest;

public class CancelDecommissionServlet extends BaseServlet {
  private final CoordinatorServer coordinator;

  public CancelDecommissionServlet(CoordinatorServer coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected Response handlePost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    CancelDecommissionRequest params = parseParamsFromJson(req, CancelDecommissionRequest.class);
    if (CollectionUtils.isEmpty(params.getServerIds())) {
      return Response.fail("Parameter[serverIds] should not be null!");
    }
    ClusterManager clusterManager = coordinator.getClusterManager();
    params.getServerIds().forEach((serverId) -> {
      clusterManager.cancelDecommission(serverId);
    });
    return Response.success(null);
  }
}
