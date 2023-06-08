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

package org.apache.uniffle.common.web.resource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import org.apache.uniffle.common.exception.InvalidRequestException;

@Path("/metrics")
public class MetricResource {
  @Context
  private HttpServletRequest httpRequest;

  @Context
  protected ServletContext servletContext;

  @GET
  @Path("/{type}")
  @Produces({ MediaType.APPLICATION_JSON })
  public MetricsJsonObj metrics(@PathParam("type") String type) {
    Enumeration<Collector.MetricFamilySamples> mfs =
        getCollectorRegistry(type).filteredMetricFamilySamples(this.parse(httpRequest));
    List<Collector.MetricFamilySamples.Sample> metrics = new LinkedList<>();
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
      metrics.addAll(metricFamilySamples.samples);
    }
    return new MetricsJsonObj(metrics, System.currentTimeMillis());
  }

  private CollectorRegistry getCollectorRegistry(String type) {
    CollectorRegistry registry = (CollectorRegistry) servletContext.getAttribute(
        CollectorRegistry.class.getCanonicalName() + "#" + type);
    if (registry == null) {
      throw new InvalidRequestException(String.format("Metric type[%s] not supported", type));
    }
    return registry;
  }

  private static class MetricsJsonObj {

    private final List<Collector.MetricFamilySamples.Sample> metrics;
    private final long timeStamp;

    MetricsJsonObj(List<Collector.MetricFamilySamples.Sample> metrics, long timeStamp) {
      this.metrics = metrics;
      this.timeStamp = timeStamp;
    }

    public List<Collector.MetricFamilySamples.Sample> getMetrics() {
      return metrics;
    }

    public long getTimeStamp() {
      return timeStamp;
    }

  }

  private Set<String> parse(HttpServletRequest req) {
    String[] includedParam = req.getParameterValues("name[]");
    return includedParam == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(includedParam));
  }
}
