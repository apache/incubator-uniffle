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

package com.tencent.rss.server;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HealthCheck will check every server whether has the ability to process shuffle data. Currently, we only support disk
 * checker. If enough disks don't have enough disk space, server will become unhealthy, and only enough disks
 * have enough disk space, server will become healthy again.
 **/
public class HealthCheck {

  private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

  private final AtomicBoolean isHealthy;
  private final long checkIntervalMs;
  private final Thread thread;
  private volatile boolean isStop = false;
  private List<Checker> checkers = Lists.newArrayList();

  public HealthCheck(AtomicBoolean isHealthy, ShuffleServerConf conf) {
    this.isHealthy = isHealthy;
    this.checkIntervalMs = conf.getLong(ShuffleServerConf.HEALTH_CHECK_INTERVAL);
    String checkersStr = conf.getString(ShuffleServerConf.HEALTH_CHECKER_CLASS_NAMES);
    if (StringUtils.isEmpty(checkersStr)) {
      throw new IllegalArgumentException("The checkers cannot be empty");
    }
    String[] checkerNames = checkersStr.split(",");
    try {
      for (String name : checkerNames) {
        Class<?> cls = Class.forName(name);
        Constructor<?> cons = cls.getConstructor(ShuffleServerConf.class);
        checkers.add((Checker)cons.newInstance(conf));
      }
    } catch (Exception e) {
      LOG.error("HealthCheck fail to init checkers", e);
      throw new IllegalArgumentException("The checkers init fail");
    }
    this.thread = new Thread(() -> {
      while (!isStop) {
        try {
          check();
          Uninterruptibles.sleepUninterruptibly(checkIntervalMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.error("HealthCheck encounter the exception", e);
        }
      }
    });
    thread.setName("HealthCheckService");
    thread.setDaemon(true);
  }

  @VisibleForTesting
  void check() {
    for (Checker checker : checkers) {
      if (!checker.checkIsHealthy()) {
        isHealthy.set(false);
        return;
      }
    }
    isHealthy.set(true);
  }

  public void start() {
    thread.start();
  }

  public void stop() {
    isStop = true;
  }
}
