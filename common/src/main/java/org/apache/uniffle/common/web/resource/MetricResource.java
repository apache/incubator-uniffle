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

import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.servlet.ServletContext;

import io.prometheus.client.Collector;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

@Path("/metrics")
@Produces({MediaType.APPLICATION_JSON})
public class MetricResource extends BaseMetricResource {
  @Context protected ServletContext servletContext;

  @GET
  @Path("/")
  public MetricsJsonObj metrics(@QueryParam("name[]") Set<String> names) {
    return metrics(null, names);
  }

  @GET
  @Path("/{type}")
  public MetricsJsonObj metrics(
      @PathParam("type") String type, @QueryParam("name[]") Set<String> names) {
    Enumeration<Collector.MetricFamilySamples> mfs =
        getCollectorRegistry(servletContext, type).filteredMetricFamilySamples(names);
    List<Collector.MetricFamilySamples.Sample> metrics = new LinkedList<>();
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
      metrics.addAll(metricFamilySamples.samples);
    }
    return new MetricsJsonObj(metrics, System.currentTimeMillis());
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
}
