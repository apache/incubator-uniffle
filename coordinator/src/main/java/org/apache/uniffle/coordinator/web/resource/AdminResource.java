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

import java.util.List;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.access.checker.AccessChecker;
import org.apache.uniffle.coordinator.web.Response;

@Produces({MediaType.APPLICATION_JSON})
public class AdminResource extends BaseResource {
  private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);
  @Context protected ServletContext servletContext;

  @GET
  @Path("/refreshChecker")
  public Response<List<ServerNode>> refreshChecker() {
    return execute(
        () -> {
          List<AccessChecker> accessCheckers = getAccessManager().getAccessCheckers();
          LOG.info(
              "The access checker {} has been refreshed, you can add the checker via rss.coordinator.access.checkers.",
              accessCheckers);
          accessCheckers.forEach(AccessChecker::refreshAccessChecker);
          return null;
        });
  }

  private AccessManager getAccessManager() {
    return (AccessManager) servletContext.getAttribute(AccessManager.class.getCanonicalName());
  }
}
