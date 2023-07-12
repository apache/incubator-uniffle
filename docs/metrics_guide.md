---
layout: page
displayTitle: Metrics Guide
title: Metrics Guide
description: Metrics Guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
# Metric Guide

## Summary
This document will introduce how to collect metrics from servers.

### Fetch metrics by REST API
``` shell
# For json format
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/metric # fetch all metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/metric/server # only fetch server metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/metric/grpc # only fetch grpc metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/metric/jvm # only fetch jvm metrics

# For Prometheus format
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/prometheus/ # fetch all metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/prometheus/metric/server # only fetch server metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/prometheus/metric/grpc # only fetch grpc metrics
curl http://${SERVER_HOST}:${SERVER_HTTP_PORT}/prometheus/metric/jvm # only fetch jvm metrics

```

### Report metrics to Prometheus automatically
PrometheusPushGatewayMetricReporter is one of the built-in metrics reporter, which will allow user pushes metrics to a [Prometheus Pushgateway](https://github.com/prometheus/pushgateway), which can be scraped by Prometheus.

|Property Name|Default| 	Description                                                                                                                                                                                                                                                                                                                          |
|---|---|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|rss.metrics.reporter.class|org.apache.uniffle.common.metrics.<br/>prometheus.PrometheusPushGatewayMetricReporter|The class of metrics reporter.|
|rss.metrics.prometheus.pushgateway.addr|-| The PushGateway server host URL including scheme, host name, and port.                                                                                                                                                                                                                                                                |
|rss.metrics.prometheus.pushgateway.groupingkey|-| Specifies the grouping key which is the group and global labels of all metrics. The label name and value are separated by '=', and labels are separated by ';', e.g., k1=v1;k2=v2. Please ensure that your grouping key meets the [Prometheus requirements](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels). |
|rss.metrics.prometheus.pushgateway.jobname|-| The job name under which metrics will be pushed.                                                                                                                                                                                                                                                                                      |
|rss.metrics.prometheus.pushgateway.report.interval.seconds|10| The interval in seconds for the reporter to report metrics.                                                                                                                                                                                                                                                                                     |

