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

package org.apache.uniffle.metrics.prometheus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.metrics.MetricReporter;
import org.apache.uniffle.metrics.MetricReporterFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PrometheusPushGatewayMetricReporterTest {

  @Test
  public void testParseGroupingKey() {
    Map<String, String> groupingKey =
        PrometheusPushGatewayMetricReporter.parseGroupingKey("k1=v1;k2=v2");
    assertNotNull(groupingKey);
    assertEquals("v1", groupingKey.get("k1"));
    assertEquals("v2", groupingKey.get("k2"));
  }

  @Test
  public void testParseIncompleteGroupingKey() {
    Map<String, String> groupingKey =
        PrometheusPushGatewayMetricReporter.parseGroupingKey("k1=");
    assertTrue(groupingKey.isEmpty());

    groupingKey = PrometheusPushGatewayMetricReporter.parseGroupingKey("=v1");
    assertTrue(groupingKey.isEmpty());

    groupingKey = PrometheusPushGatewayMetricReporter.parseGroupingKey("k1");
    assertTrue(groupingKey.isEmpty());
  }

  @Test
  public void test() throws Exception {
    RssConf conf = new RssConf();
    conf.setString(MetricReporterFactory.REPORT_CLASS, PrometheusPushGatewayMetricReporter.class.getCanonicalName());
    conf.setString(PrometheusPushGatewayMetricReporter.PUSHGATEWAY_ADDR, "");
    conf.setString(PrometheusPushGatewayMetricReporter.GROUPING_KEY, "a=1;b=2");
    String jobName = "jobname";
    conf.setString(PrometheusPushGatewayMetricReporter.JOB_NAME, jobName);
    MetricReporter metricReporter = MetricReporterFactory.getMetricReporter(conf);
    assertTrue(metricReporter instanceof PrometheusPushGatewayMetricReporter);
    MetricsManager metricsManager = new MetricsManager();
    CollectorRegistry collectorRegistry = metricsManager.getCollectorRegistry();
    metricReporter.addCollectorRegistry(collectorRegistry);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Counter counter1 = metricsManager.addCounter("counter1");
    counter1.inc();
    PushGateway pushGateway = new CustomPushGateway((registry, job, groupingKey) -> {
      countDownLatch.countDown();
      assertEquals(job, jobName);
      assertEquals(2, groupingKey.size());
      assertEquals(1, counter1.get());
    });
    ((PrometheusPushGatewayMetricReporter) metricReporter).setPushGateway(pushGateway);
    metricReporter.start();
    countDownLatch.await(20, TimeUnit.SECONDS);
    metricReporter.stop();
  }

  class CustomPushGateway extends PushGateway {

    private final CustomCallback<CollectorRegistry, String, Map<String, String>> callback;

    CustomPushGateway(CustomCallback<CollectorRegistry, String, Map<String, String>> callback) {
      super("localhost");
      this.callback = callback;
    }

    @Override
    public void push(CollectorRegistry registry, String job, Map<String, String> groupingKey) throws IOException {
      callback.apply(registry, job, groupingKey);
    }
  }

  @FunctionalInterface
  interface CustomCallback<P1, P2, P3> {
    void apply(P1 p1, P2 p2, P3 p3);
  }
}
