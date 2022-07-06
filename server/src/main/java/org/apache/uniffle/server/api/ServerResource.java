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
package org.apache.uniffle.server.api;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.uniffle.server.ShuffleServer;

@Path("/server")
public class ServerResource {

    @Context
    private ServletContext servletContext;

    @GET
    @Path("/isHealthy")
    @Produces(MediaType.APPLICATION_JSON)
    public Response isHealthy() {
        return Response.ok(getShuffleServer().isHealthy()).build();
    }

    @GET
    @Path("/offline")
    @Produces(MediaType.APPLICATION_JSON)
    public Response offline() {
        getShuffleServer().offline();
        return Response.ok().build();
    }

    private ShuffleServer getShuffleServer() {
        return (ShuffleServer) servletContext.getAttribute(ShuffleServer.SERVLET_CONTEXT_KEY);
    }
}
