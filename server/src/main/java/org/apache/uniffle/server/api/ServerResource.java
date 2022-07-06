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
