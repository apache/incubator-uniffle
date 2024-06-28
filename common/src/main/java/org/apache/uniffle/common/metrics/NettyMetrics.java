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

public abstract class NettyMetrics extends RPCMetrics {

  private static final String NETTY_ACTIVE_CONNECTION = "netty_active_connection";
  private static final String NETTY_HANDLE_EXCEPTION = "netty_handle_exception";
  private static final String NETTY_PENDING_TASKS_NUM_FOR_BOSS_GROUP =
      "netty_pending_tasks_num_for_boss_group";
  private static final String NETTY_PENDING_TASKS_NUM_FOR_WORKER_GROUP =
      "netty_pending_tasks_num_for_worker_group";

  protected Gauge.Child gaugeNettyActiveConn;
  protected Counter.Child counterNettyException;
  protected Gauge.Child gaugeNettyPendingTasksNumForBossGroup;
  protected Gauge.Child gaugeNettyPendingTasksNumForWorkerGroup;

  public NettyMetrics(RssConf rssConf, String tags) {
    super(rssConf, tags);
  }

  @Override
  public void registerGeneralMetrics() {
    gaugeNettyActiveConn = metricsManager.addLabeledGauge(NETTY_ACTIVE_CONNECTION);
    counterNettyException = metricsManager.addLabeledCounter(NETTY_HANDLE_EXCEPTION);
    gaugeNettyPendingTasksNumForBossGroup =
        metricsManager.addLabeledGauge(NETTY_PENDING_TASKS_NUM_FOR_BOSS_GROUP);
    gaugeNettyPendingTasksNumForWorkerGroup =
        metricsManager.addLabeledGauge(NETTY_PENDING_TASKS_NUM_FOR_WORKER_GROUP);
  }

  public Counter.Child getCounterNettyException() {
    return counterNettyException;
  }

  public Gauge.Child getGaugeNettyActiveConn() {
    return gaugeNettyActiveConn;
  }

  public Gauge.Child getGaugeNettyPendingTasksNumForBossGroup() {
    return gaugeNettyPendingTasksNumForBossGroup;
  }

  public Gauge.Child getGaugeNettyPendingTasksNumForWorkerGroup() {
    return gaugeNettyPendingTasksNumForWorkerGroup;
  }
}
