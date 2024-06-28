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

package org.apache.uniffle.dashboard.web.resource;

import java.util.Map;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;

@Produces({MediaType.APPLICATION_JSON})
public class CoordinatorResource extends BaseResource {

  @Context protected ServletContext servletContext;

  @GET
  @Path("/coordinatorServers")
  public Response<Map<String, String>> getCoordinatorServers() {
    return execute(
        () -> {
          Map<String, String> coordinatorServerAddresses = getCoordinatorServerAddresses();
          return coordinatorServerAddresses;
        });
  }

  private Map<String, String> getCoordinatorServerAddresses() {
    return (Map<String, String>) servletContext.getAttribute("coordinatorServerAddresses");
  }
}
