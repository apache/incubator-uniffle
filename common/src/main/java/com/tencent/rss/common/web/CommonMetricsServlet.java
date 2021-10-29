/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.common.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CommonMetricsServlet extends MetricsServlet {

  final boolean isPrometheus;
  private CollectorRegistry registry;

  public CommonMetricsServlet(CollectorRegistry registry) {
    this(registry, false);
  }

  public CommonMetricsServlet(CollectorRegistry registry, boolean isPrometheus) {
    super(registry);
    this.registry = registry;
    this.isPrometheus = isPrometheus;
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (isPrometheus) {
      super.doGet(req, resp);
    } else {
      resp.setStatus(200);
      resp.setContentType("text/plain; version=0.0.4; charset=utf-8");

      try (BufferedWriter writer = new BufferedWriter(resp.getWriter())) {
        toJson(writer, getSamples(req));
        writer.flush();
      }
    }
  }

  private Set<String> parse(HttpServletRequest req) {
    String[] includedParam = req.getParameterValues("name[]");
    return includedParam == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(includedParam));
  }

  private Enumeration<Collector.MetricFamilySamples> getSamples(HttpServletRequest req) {
    return this.registry.filteredMetricFamilySamples(this.parse(req));
  }

  public void toJson(Writer writer, Enumeration<Collector.MetricFamilySamples> mfs) throws IOException {

    List<Collector.MetricFamilySamples.Sample> metrics = new LinkedList<>();
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
      metrics.addAll(metricFamilySamples.samples);
    }

    MetricsJsonObj res = new MetricsJsonObj(metrics, System.currentTimeMillis());
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(res);
    writer.write(json);
  }

  private static class MetricsJsonObj {

    private final List<Collector.MetricFamilySamples.Sample> metrics;
    private final long timeStamp;

    MetricsJsonObj(List<Collector.MetricFamilySamples.Sample> metrics, long timeStamp) {
      this.metrics = metrics;
      this.timeStamp = timeStamp;
    }

    public List<Sample> getMetrics() {
      return metrics;
    }

    public long getTimeStamp() {
      return timeStamp;
    }

  }
}
