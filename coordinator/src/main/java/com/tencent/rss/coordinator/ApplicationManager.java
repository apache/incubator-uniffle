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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.Constants;

public class ApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  private long expired;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  // store appId -> remote path to make sure all shuffle data of the same application
  // will be written to the same remote storage
  private Map<String, String> appIdToRemoteStoragePath = Maps.newConcurrentMap();
  // store remote path -> application count for assignment strategy
  private Map<String, AtomicInteger> remoteStoragePathCounter = Maps.newConcurrentMap();
  private Set<String> availableRemoteStoragePath = Sets.newConcurrentHashSet();
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

  public void refreshRemoteStorage(String remoteStoragePath) {
    if (!StringUtils.isEmpty(remoteStoragePath)) {
      LOG.info("Refresh remote storage with {}", remoteStoragePath);
      Set<String> paths = Sets.newHashSet(remoteStoragePath.split(Constants.COMMA_SPLIT_CHAR));
      // add remote path if not exist
      for (String path : paths) {
        if (!availableRemoteStoragePath.contains(path)) {
          remoteStoragePathCounter.putIfAbsent(path, new AtomicInteger(0));
          availableRemoteStoragePath.add(path);
        }
      }
      // remove unused remote path if exist
      List<String> unusedPath = Lists.newArrayList();
      for (String existPath : availableRemoteStoragePath) {
        if (!paths.contains(existPath)) {
          unusedPath.add(existPath);
        }
      }
      // remote unused path
      for (String path : unusedPath) {
        availableRemoteStoragePath.remove(path);
        // try to remove if counter = 0, or it will be removed in decRemoteStorageCounter() later
        removePathFromCounter(path);
      }
    } else {
      LOG.info("Refresh remote storage with empty value {}", remoteStoragePath);
      for (String path : availableRemoteStoragePath) {
        removePathFromCounter(path);
      }
      availableRemoteStoragePath.clear();
    }
  }

  // the strategy of pick remote storage is according to assignment count
  // todo: better strategy with workload balance
  public String pickRemoteStoragePath(String appId) {
    if (appIdToRemoteStoragePath.containsKey(appId)) {
      return appIdToRemoteStoragePath.get(appId);
    }

    // create list for sort
    List<Map.Entry<String, AtomicInteger>> sizeList =
            Lists.newArrayList(remoteStoragePathCounter.entrySet());

    sizeList.sort((entry1, entry2) -> {
      if (entry1 == null && entry2 == null) {
        return 0;
      }
      if (entry1 == null) {
        return -1;
      }
      if (entry2 == null) {
        return 1;
      }
      if (entry1.getValue().get() > entry2.getValue().get()) {
        return 1;
      } else if (entry1.getValue().get() == entry2.getValue().get()) {
        return 0;
      }
      return -1;
    });

    for (Map.Entry<String, AtomicInteger> entry : sizeList) {
      String storagePath = entry.getKey();
      if (availableRemoteStoragePath.contains(storagePath)) {
        appIdToRemoteStoragePath.putIfAbsent(appId, storagePath);
        incRemoteStorageCounter(storagePath);
        break;
      }
    }
    return appIdToRemoteStoragePath.get(appId);
  }

  @VisibleForTesting
  protected synchronized void incRemoteStorageCounter(String remoteStoragePath) {
    AtomicInteger counter = remoteStoragePathCounter.get(remoteStoragePath);
    if (counter != null) {
      counter.incrementAndGet();
    } else {
      // it may be happened when assignment remote storage
      // and refresh remote storage at the same time
      LOG.warn("Remote storage path lost during assignment: %s doesn't exist, reset it to 1",
              remoteStoragePath);
      remoteStoragePathCounter.put(remoteStoragePath, new AtomicInteger(1));
    }
  }

  @VisibleForTesting
  protected synchronized void decRemoteStorageCounter(String storagePath) {
    AtomicInteger atomic = remoteStoragePathCounter.get(storagePath);
    if (atomic != null) {
      int count = atomic.decrementAndGet();
      if (count < 0) {
        LOG.warn("Unexpected counter for remote storage: %s, which is %i, reset to 0",
                storagePath, count);
        atomic.set(0);
      }
    } else {
      LOG.warn("Can't find counter for remote storage: {}", storagePath);
      remoteStoragePathCounter.putIfAbsent(storagePath, new AtomicInteger(0));
    }
    if (remoteStoragePathCounter.get(storagePath).get() == 0
            && !availableRemoteStoragePath.contains(storagePath)) {
      remoteStoragePathCounter.remove(storagePath);
    }
  }

  private synchronized void removePathFromCounter(String storagePath) {
    AtomicInteger atomic = remoteStoragePathCounter.get(storagePath);
    if (atomic != null && atomic.get() == 0) {
      remoteStoragePathCounter.remove(storagePath);
    }
  }

  public Set<String> getAppIds() {
    return appIds.keySet();
  }

  @VisibleForTesting
  protected Map<String, String> getAppIdToRemoteStoragePath() {
    return appIdToRemoteStoragePath;
  }

  @VisibleForTesting
  protected Map<String, AtomicInteger> getRemoteStoragePathCounter() {
    return remoteStoragePathCounter;
  }

  @VisibleForTesting
  public Set<String> getAvailableRemoteStoragePath() {
    return availableRemoteStoragePath;
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
        decRemoteStorageCounter(appIdToRemoteStoragePath.get(appId));
        appIdToRemoteStoragePath.remove(appId);
      }
      CoordinatorMetrics.gaugeRunningAppNum.set(appIds.size());
    } catch (Exception e) {
      LOG.warn("Error happened in statusCheck", e);
    }
  }
}
