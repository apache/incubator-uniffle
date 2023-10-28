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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public abstract class RPCMetrics {
  protected boolean isRegistered = false;
  protected Map<String, Counter.Child> counterMap = JavaUtils.newConcurrentMap();
  protected Map<String, Gauge.Child> gaugeMap = JavaUtils.newConcurrentMap();
  protected Map<String, Summary.Child> transportTimeSummaryMap = JavaUtils.newConcurrentMap();
  protected Map<String, Summary.Child> processTimeSummaryMap = JavaUtils.newConcurrentMap();
  private final ExecutorService summaryObservePool;
  protected MetricsManager metricsManager;
  protected String tags;

  public RPCMetrics(String tags) {
    this.tags = tags;
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(1000);
    this.summaryObservePool = new ThreadPoolExecutor(2, 10, 60,
      TimeUnit.SECONDS, waitQueue,
      ThreadUtils.getThreadFactory("SummaryObserveThreadPool-%d"), new ThreadPoolExecutor.DiscardPolicy());
  }

  public abstract void registerMetrics();

  public abstract void registerGeneralMetrics();

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

  public void incCounter(String metricKey) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(metricKey);
      if (gauge != null) {
        gauge.inc();
      }
      Counter.Child counter = counterMap.get(metricKey);
      if (counter != null) {
        counter.inc();
      }
    }
  }

  public void decCounter(String metricKey) {
    if (isRegistered) {
      Gauge.Child gauge = gaugeMap.get(metricKey);
      if (gauge != null) {
        gauge.dec();
      }
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
      summaryObservePool.execute(
        () -> summary.observe(processTimeInMillionSecond / Constants.MILLION_SECONDS_PER_SECOND));
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

  public Map<String, Summary.Child> getTransportTimeSummaryMap() {
    return transportTimeSummaryMap;
  }

  public Map<String, Summary.Child> getProcessTimeSummaryMap() {
    return processTimeSummaryMap;
  }
}
