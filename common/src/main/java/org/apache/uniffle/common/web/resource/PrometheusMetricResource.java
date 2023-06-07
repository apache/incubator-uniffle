package org.apache.uniffle.common.web.resource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

@Path("/prometheus/metrics")
public class PrometheusMetricResource {
  @Context
  private HttpServletRequest httpRequest;
  @Context
  private HttpServletResponse httpServletResponse;

  @Context
  protected ServletContext servletContext;

  @GET
  @Path("/{type}")
  public String metrics(@PathParam("type") String type) throws IOException {
    httpServletResponse.setStatus(200);
    httpServletResponse.setContentType("text/plain; version=0.0.4; charset=utf-8");
    Writer writer = new BufferedWriter(httpServletResponse.getWriter());

    try {
      TextFormat.write004(writer, getCollectorRegistry(type).filteredMetricFamilySamples(this.parse(httpRequest)));
      writer.flush();
    } finally {
      writer.close();
    }
    return null;
  }

  private CollectorRegistry getCollectorRegistry(String type) {
    CollectorRegistry registry = (CollectorRegistry) servletContext.getAttribute(
        CollectorRegistry.class.getCanonicalName() + "#" + type);
    if (registry == null) {
      throw new RuntimeException(String.format("Metric type[%s] not supported", type));
    }
    return registry;
  }

  private Set<String> parse(HttpServletRequest req) {
    String[] includedParam = req.getParameterValues("name[]");
    return includedParam == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(includedParam));
  }
}
