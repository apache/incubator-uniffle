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

import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;

public class NettyDirectMemoryTracker {

  private static final Logger LOG = LoggerFactory.getLogger(NettyDirectMemoryTracker.class);

  private ShuffleBufferManager shuffleBufferManager;
  private final long reportInitialDelay;
  private final long reportInterval;
  private boolean nettyServerEnabled;
  private final ScheduledExecutorService service =
      Executors.newSingleThreadScheduledExecutor(
          ThreadUtils.getThreadFactory("NettyDirectMemoryTracker"));

  public NettyDirectMemoryTracker(
      ShuffleServerConf conf, ShuffleBufferManager shuffleBufferManager) {
    this.nettyServerEnabled = conf.get(ShuffleServerConf.NETTY_SERVER_PORT) >= 0;
    this.shuffleBufferManager = shuffleBufferManager;
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
            long usedDirectMemory = PlatformDependent.usedDirectMemory();
            long allocatedDirectMemory =
                NettyUtils.getNettyBufferAllocator().metric().usedDirectMemory();
            long pinnedDirectMemory = NettyUtils.getNettyBufferAllocator().pinnedDirectMemory();
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Current usedDirectMemory:{}, allocatedDirectMemory:{}, pinnedDirectMemory:{}",
                  usedDirectMemory,
                  allocatedDirectMemory,
                  pinnedDirectMemory);
            }
            ShuffleServerMetrics.gaugeUsedDirectMemorySize.set(usedDirectMemory);
            ShuffleServerMetrics.gaugeAllocatedDirectMemorySize.set(allocatedDirectMemory);
            ShuffleServerMetrics.gaugePinnedDirectMemorySize.set(pinnedDirectMemory);
            if (nettyServerEnabled) {
              shuffleBufferManager.setUsedMemory(pinnedDirectMemory);
              ShuffleServerMetrics.gaugeUsedBufferSize.set(pinnedDirectMemory);
            }
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
