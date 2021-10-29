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

package com.tencent.rss.common.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class MetricsManager {
  private CollectorRegistry collectorRegistry;

  public MetricsManager() {
    this(null);
  }

  public MetricsManager(CollectorRegistry collectorRegistry) {
    if (collectorRegistry == null) {
      this.collectorRegistry = CollectorRegistry.defaultRegistry;
    } else {
      this.collectorRegistry = collectorRegistry;
    }
  }

  public CollectorRegistry getCollectorRegistry() {
    return this.collectorRegistry;
  }

  public Counter addCounter(String name, String... labels) {
    return addCounter(name, "Counter " + name, labels);
  }

  public Counter addCounter(String name, String help, String[] labels) {
    return Counter.build().name(name).labelNames(labels).help(help).register(collectorRegistry);
  }

  public Gauge addGauge(String name, String... labels) {
    return addGauge(name, "Gauge " + name, labels);
  }

  public Gauge addGauge(String name, String help, String[] labels) {
    return Gauge.build().name(name).labelNames(labels).help(help).register(collectorRegistry);
  }

  public Histogram addHistogram(String name, double[] buckets, String... labels) {
    return addHistogram(name, "Histogram " + name, buckets, labels);
  }

  public Histogram addHistogram(String name, String help, double[] buckets, String[] labels) {
    return Histogram.build().name(name).buckets(buckets).labelNames(labels).help(help).register(collectorRegistry);
  }
}
