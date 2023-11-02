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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.Constants;

public abstract class GRPCMetrics extends RPCMetrics {
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

  protected Gauge.Child gaugeGrpcOpen;
  protected Counter.Child counterGrpcTotal;

  public GRPCMetrics(RssConf rssConf, String tags) {
    super(rssConf, tags);
  }

  @Override
  public abstract void registerMetrics();

  @Override
  public void registerGeneralMetrics() {
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

  @Override
  public void incCounter(String methodName) {
    if (isRegistered) {
      super.incCounter(methodName);
      gaugeGrpcOpen.inc();
      counterGrpcTotal.inc();
    }
  }

  @Override
  public void decCounter(String methodName) {
    if (isRegistered) {
      super.decCounter(methodName);
      gaugeGrpcOpen.dec();
    }
  }

  public Gauge.Child getGaugeGrpcOpen() {
    return gaugeGrpcOpen;
  }

  public Counter.Child getCounterGrpcTotal() {
    return counterGrpcTotal;
  }

  public static GRPCMetrics getEmptyGRPCMetrics(RssConf rssConf) {
    return new EmptyGRPCMetrics(rssConf, Constants.SHUFFLE_SERVER_VERSION);
  }
}
