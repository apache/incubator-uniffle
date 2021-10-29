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

package com.tencent.rss.coordinator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  private long expired;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  private ScheduledExecutorService scheduledExecutorService;

  public ApplicationManager(CoordinatorConf conf) {
    expired = conf.getLong(CoordinatorConf.COORDINATOR_APP_EXPIRED);
    // the thread for checking application status
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ApplicationManager-%d").build());
    scheduledExecutorService.scheduleAtFixedRate(
        () -> statusCheck(), expired / 2, expired / 2, TimeUnit.MILLISECONDS);
  }

  public void refreshAppId(String appId) {
    if (!appIds.containsKey(appId)) {
      CoordinatorMetrics.counterTotalAppNum.inc();
    }
    appIds.put(appId, System.currentTimeMillis());
  }

  public Set<String> getAppIds() {
    return appIds.keySet();
  }

  private void statusCheck() {
    try {
      LOG.info("Start to check status for " + appIds.size() + " applications");
      long current = System.currentTimeMillis();
      Set<String> expiredAppIds = Sets.newHashSet();
      for (Map.Entry<String, Long> entry : appIds.entrySet()) {
        long lastReport = entry.getValue();
        if (current - lastReport > expired) {
          expiredAppIds.add(entry.getKey());
        }
      }
      for (String appId : expiredAppIds) {
        LOG.info("Remove expired application:" + appId);
        appIds.remove(appId);
      }
      CoordinatorMetrics.gaugeRunningAppNum.set(appIds.size());
    } catch (Exception e) {
      LOG.warn("Error happened in statusCheck", e);
    }
  }
}
