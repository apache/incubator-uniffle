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
import io.prometheus.client.Summary;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;

public abstract class GRPCMetrics {
  // Grpc server internal executor metrics
  public static final String GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY =
      "grpcServerExecutorActiveThreads";
  private static final String GRPC_SERVER_EXECUTOR_ACTIVE_THREADS =
      "grpc_server_executor_active_threads";
  public static final String GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY =
      "grpcServerExecutorBlockingQueueSize";
  private static final String GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE =
      "grpc_server_executor_blocking_queue_size";
  public static final String GRPC_SERVER_CONNECTION_NUMBER_KEY = "grpcServerConnectionNumber";
  private static final String GRPC_SERVER_CONNECTION_NUMBER = "grpc_server_connection_number";
  private static final String GRPC_OPEN = "grpc_open";
  private static final String GRPC_TOTAL = "grpc_total";

  private boolean isRegistered = false;
  protected Map<String, Counter.Child> counterMap = JavaUtils.newConcurrentMap();
  protected Map<String, Gauge.Child> gaugeMap = JavaUtils.newConcurrentMap();
  protected Map<String, Summary.Child> transportTimeSummaryMap = JavaUtils.newConcurrentMap();
  protected Map<String, Summary.Child> processTimeSummaryMap = JavaUtils.newConcurrentMap();
  protected Gauge.Child gaugeGrpcOpen;
  protected Counter.Child counterGrpcTotal;
  protected MetricsManager metricsManager;
  protected String tags;

  public GRPCMetrics(String tags) {
    this.tags = tags;
  }

  public abstract void registerMetrics();

  public void register(CollectorRegistry collectorRegistry) {
    if (!isRegistered) {
      Map<String, String> labels = Maps.newHashMap();
      labels.put(Constants.METRICS_TAG_LABEL_NAME, tags);
      metricsManager = new MetricsManager(collectorRegistry, labels);
      registerGeneralMetrics();
      registerMetrics();
      isRegistered = true;
    }
  }

  private void registerGeneralMetrics() {
    gaugeGrpcOpen = metricsManager.addLabeledGauge(GRPC_OPEN);
    counterGrpcTotal = metricsManager.addLabeledCounter(GRPC_TOTAL);
    gaugeMap.putIfAbsent(
        GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY,
        metricsManager.addLabeledGauge(GRPC_SERVER_EXECUTOR_ACTIVE_THREADS));
    gaugeMap.putIfAbsent(
        GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY,
        metricsManager.addLabeledGauge(GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE));
    gaugeMap.putIfAbsent(
        GRPC_SERVER_CONNECTION_NUMBER_KEY,
        metricsManager.addLabeledGauge(GRPC_SERVER_CONNECTION_NUMBER));
  }

  public void setGauge(String tag, double value) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(tag);
      if (gauge != null) {
        gauge.set(value);
      }
    }
  }

  public void incGauge(String tag) {
    incGauge(tag, 1);
  }

  public void incGauge(String tag, double value) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(tag);
      if (gauge != null) {
        gauge.inc(value);
      }
    }
  }

  public void decGauge(String tag) {
    decGauge(tag, 1);
  }

  public void decGauge(String tag, double value) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(tag);
      if (gauge != null) {
        gauge.dec(value);
      }
    }
  }

  public void incCounter(String methodName) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.inc();
      }
      Counter.Child counter = counterMap.get(methodName);
      if (counter != null) {
        counter.inc();
      }
      gaugeGrpcOpen.inc();
      counterGrpcTotal.inc();
    }
  }

  public void decCounter(String methodName) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.dec();
      }
      gaugeGrpcOpen.dec();
    }
  }

  public void recordTransportTime(String methodName, long transportTimeInMillionSecond) {
    Summary.Child summary = transportTimeSummaryMap.get(methodName);
    if (summary != null) {
      summary.observe(transportTimeInMillionSecond / Constants.MILLION_SECONDS_PER_SECOND);
    }
  }

  public void recordProcessTime(String methodName, long processTimeInMillionSecond) {
    Summary.Child summary = processTimeSummaryMap.get(methodName);
    if (summary != null) {
      summary.observe(processTimeInMillionSecond / Constants.MILLION_SECONDS_PER_SECOND);
    }
  }

  public CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public Map<String, Counter.Child> getCounterMap() {
    return counterMap;
  }

  public Map<String, Gauge.Child> getGaugeMap() {
    return gaugeMap;
  }

  public Gauge.Child getGaugeGrpcOpen() {
    return gaugeGrpcOpen;
  }

  public Counter.Child getCounterGrpcTotal() {
    return counterGrpcTotal;
  }

  public Map<String, Summary.Child> getTransportTimeSummaryMap() {
    return transportTimeSummaryMap;
  }

  public Map<String, Summary.Child> getProcessTimeSummaryMap() {
    return processTimeSummaryMap;
  }

  public static GRPCMetrics getEmptyGRPCMetrics() {
    return new EmptyGRPCMetrics(Constants.SHUFFLE_SERVER_VERSION);
  }
}
