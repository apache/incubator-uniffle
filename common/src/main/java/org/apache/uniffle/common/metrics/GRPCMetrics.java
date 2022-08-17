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

package org.apache.uniffle.common.metrics;

import java.util.Map;

import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public abstract class GRPCMetrics {
  // Grpc server internal executor metrics
  public static final String GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY = "grpcServerExecutorActiveThreads";
  private static final String GRPC_SERVER_EXECUTOR_ACTIVE_THREADS = "grpc_server_executor_active_threads";
  public static final String GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY = "grpcServerExecutorBlockingQueueSize";
  private static final String GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE = "grpc_server_executor_blocking_queue_size";
  public static final String GRCP_SERVER_CONNECTION_NUMBER_KEY = "grpcServerConnectionNumber";
  private static final String GRCP_SERVER_CONNECTION_NUMBER = "grpc_server_connection_number";

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
      registerGeneralMetrics();
      registerMetrics();
      isRegister = true;
    }
  }

  private void registerGeneralMetrics() {
    gaugeMap.putIfAbsent(
        GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY,
        metricsManager.addGauge(GRPC_SERVER_EXECUTOR_ACTIVE_THREADS)
    );
    gaugeMap.putIfAbsent(
        GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY,
        metricsManager.addGauge(GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE)
    );
    gaugeMap.putIfAbsent(
        GRCP_SERVER_CONNECTION_NUMBER_KEY,
        metricsManager.addGauge(GRCP_SERVER_CONNECTION_NUMBER)
    );
  }

  public void setGauge(String tag, double value) {
    if (isRegister) {
      Gauge gauge = gaugeMap.get(tag);
      if (gauge != null) {
        gauge.set(value);
      }
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
