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

package org.apache.uniffle.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ThreadUtils;

public class NettyDirectMemoryTracker {

  private static final Logger LOG = LoggerFactory.getLogger(NettyDirectMemoryTracker.class);

  private final long reportInitialDelay;
  private final long reportInterval;
  private final ScheduledExecutorService service =
      Executors.newSingleThreadScheduledExecutor(
          ThreadUtils.getThreadFactory("NettyDirectMemoryTracker"));

  public NettyDirectMemoryTracker(ShuffleServerConf conf) {
    this.reportInitialDelay =
        conf.getLong(ShuffleServerConf.SERVER_NETTY_DIRECT_MEMORY_USAGE_TRACKER_DELAY);
    this.reportInterval =
        conf.getLong(ShuffleServerConf.SERVER_NETTY_DIRECT_MEMORY_USAGE_TRACKER_INTERVAL);
  }

  public void start() {
    LOG.info(
        "Start report direct memory usage to MetricSystem after {}ms and interval is {}ms",
        reportInitialDelay,
        reportInterval);

    service.scheduleAtFixedRate(
        () -> {
          try {
            long usedDirectMemoryByNetty = PlatformDependent.usedDirectMemory();
            long usedDirectMemoryByGrpcNetty =
                io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent.usedDirectMemory();
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Current usedDirectMemoryByNetty:{}, usedDirectMemoryByGrpcNetty:{}",
                  usedDirectMemoryByNetty,
                  usedDirectMemoryByGrpcNetty);
            }
            ShuffleServerMetrics.gaugeUsedDirectMemorySizeByNetty.set(usedDirectMemoryByNetty);
            ShuffleServerMetrics.gaugeUsedDirectMemorySizeByGrpcNetty.set(
                usedDirectMemoryByGrpcNetty);
            ShuffleServerMetrics.gaugeUsedDirectMemorySize.set(
                usedDirectMemoryByNetty + usedDirectMemoryByGrpcNetty);
          } catch (Throwable t) {
            LOG.error("Failed to report direct memory.", t);
          }
        },
        reportInitialDelay,
        reportInterval,
        TimeUnit.MILLISECONDS);
  }

  public void stop() {
    service.shutdownNow();
  }
}
