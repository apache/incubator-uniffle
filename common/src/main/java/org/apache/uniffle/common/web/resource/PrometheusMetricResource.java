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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import io.prometheus.client.exporter.common.TextFormat;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;

@Path("/prometheus/metrics")
public class PrometheusMetricResource extends BaseMetricResource {
  @Context private HttpServletResponse httpServletResponse;
  @Context protected ServletContext servletContext;

  @GET
  @Path("/{type}")
  public String metrics(@PathParam("type") String type, @QueryParam("name[]") Set<String> names)
      throws IOException {
    httpServletResponse.setStatus(200);
    httpServletResponse.setContentType("text/plain; version=0.0.4; charset=utf-8");
    Writer writer = new BufferedWriter(httpServletResponse.getWriter());

    try {
      TextFormat.write004(
          writer, getCollectorRegistry(servletContext, type).filteredMetricFamilySamples(names));
      writer.flush();
    } finally {
      writer.close();
    }
    return null;
  }
}
