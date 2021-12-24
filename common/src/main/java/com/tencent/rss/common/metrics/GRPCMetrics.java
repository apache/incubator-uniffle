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

import java.util.Map;

import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public abstract class GRPCMetrics {

  private boolean isRegister = false;
  protected Map<String, Counter> counterMap = Maps.newConcurrentMap();
  protected Map<String, Gauge> gaugeMap = Maps.newConcurrentMap();
  protected Gauge gaugeGrpcOpen;
  protected Counter counterGrpcTotal;
  protected MetricsManager metricsManager;

  public abstract void registerMetrics();

  public void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      registerMetrics();
      isRegister = true;
    }
  }

  public void incCounter(String methodName) {
    if (isRegister) {
      Gauge gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.inc();
      }
      Counter counter = counterMap.get(methodName);
      if (counter != null) {
        counter.inc();
      }
      gaugeGrpcOpen.inc();
      counterGrpcTotal.inc();
    }
  }

  public void decCounter(String methodName) {
    if (isRegister) {
      Gauge gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.dec();
      }
      gaugeGrpcOpen.dec();
    }
  }

  public CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public Map<String, Counter> getCounterMap() {
    return counterMap;
  }

  public Map<String, Gauge> getGaugeMap() {
    return gaugeMap;
  }

  public Gauge getGaugeGrpcOpen() {
    return gaugeGrpcOpen;
  }

  public Counter getCounterGrpcTotal() {
    return counterGrpcTotal;
  }
}
