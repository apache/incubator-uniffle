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

package org.apache.uniffle.coordinator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ThreadUtils;

public class ApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  private long expired;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  // store appId -> remote path to make sure all shuffle data of the same application
  // will be written to the same remote storage
  private Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo = Maps.newConcurrentMap();
  // store remote path -> application count for assignment strategy
  private Map<String, AtomicInteger> remoteStoragePathCounter = Maps.newConcurrentMap();
  private Map<String, String> remoteStorageToHost = Maps.newConcurrentMap();
  private Map<String, RemoteStorageInfo> availableRemoteStorageInfo = Maps.newHashMap();
  private ScheduledExecutorService scheduledExecutorService;
  // it's only for test case to check if status check has problem
  private boolean hasErrorInStatusCheck = false;

  public ApplicationManager(CoordinatorConf conf) {
    expired = conf.getLong(CoordinatorConf.COORDINATOR_APP_EXPIRED);
    // the thread for checking application status
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("ApplicationManager-%d"));
    scheduledExecutorService.scheduleAtFixedRate(
        () -> statusCheck(), expired / 2, expired / 2, TimeUnit.MILLISECONDS);
  }

  public void refreshAppId(String appId) {
    if (!appIds.containsKey(appId)) {
      CoordinatorMetrics.counterTotalAppNum.inc();
      LOG.info("New application is registered: {}", appId);
    }
    appIds.put(appId, System.currentTimeMillis());
  }

  public void refreshRemoteStorage(String remoteStoragePath, String remoteStorageConf) {
    if (!StringUtils.isEmpty(remoteStoragePath)) {
      LOG.info("Refresh remote storage with {} {}", remoteStoragePath, remoteStorageConf);
      Set<String> paths = Sets.newHashSet(remoteStoragePath.split(Constants.COMMA_SPLIT_CHAR));
      Map<String, Map<String, String>> confKVs = CoordinatorUtils.extractRemoteStorageConf(remoteStorageConf);
      // add remote path if not exist
      for (String path : paths) {
        if (!availableRemoteStorageInfo.containsKey(path)) {
          remoteStoragePathCounter.putIfAbsent(path, new AtomicInteger(0));
          // refreshRemoteStorage is designed without multiple thread problem
          // metrics shouldn't be added duplicated
          addRemoteStorageMetrics(path);
        }
        String storageHost = getStorageHost(path);
        RemoteStorageInfo rsInfo = new RemoteStorageInfo(path, confKVs.getOrDefault(storageHost, Maps.newHashMap()));
        availableRemoteStorageInfo.put(path, rsInfo);
      }
      // remove unused remote path if exist
      List<String> unusedPath = Lists.newArrayList();
      for (String existPath : availableRemoteStorageInfo.keySet()) {
        if (!paths.contains(existPath)) {
          unusedPath.add(existPath);
        }
      }
      // remote unused path
      for (String path : unusedPath) {
        availableRemoteStorageInfo.remove(path);
        // try to remove if counter = 0, or it will be removed in decRemoteStorageCounter() later
        removePathFromCounter(path);
      }
    } else {
      LOG.info("Refresh remote storage with empty value {}", remoteStoragePath);
      for (String path : availableRemoteStorageInfo.keySet()) {
        removePathFromCounter(path);
      }
      availableRemoteStorageInfo.clear();
    }
  }

  // the strategy of pick remote storage is according to assignment count
  // todo: better strategy with workload balance
  public RemoteStorageInfo pickRemoteStorage(String appId) {
    if (appIdToRemoteStorageInfo.containsKey(appId)) {
      return appIdToRemoteStorageInfo.get(appId);
    }

    // create list for sort
    List<Map.Entry<String, AtomicInteger>> sizeList =
        Lists.newArrayList(remoteStoragePathCounter.entrySet()).stream().filter(Objects::nonNull)
            .sorted(Comparator.comparingInt(entry -> entry.getValue().get())).collect(Collectors.toList());

    for (Map.Entry<String, AtomicInteger> entry : sizeList) {
      String storagePath = entry.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        appIdToRemoteStorageInfo.putIfAbsent(appId, availableRemoteStorageInfo.get(storagePath));
        incRemoteStorageCounter(storagePath);
        break;
      }
    }
    return appIdToRemoteStorageInfo.get(appId);
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
    if (!StringUtils.isEmpty(storagePath)) {
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
          && !availableRemoteStorageInfo.containsKey(storagePath)) {
        remoteStoragePathCounter.remove(storagePath);
      }
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
  protected Map<String, RemoteStorageInfo> getAppIdToRemoteStorageInfo() {
    return appIdToRemoteStorageInfo;
  }

  @VisibleForTesting
  protected Map<String, AtomicInteger> getRemoteStoragePathCounter() {
    return remoteStoragePathCounter;
  }

  @VisibleForTesting
  public Map<String, RemoteStorageInfo> getAvailableRemoteStorageInfo() {
    return availableRemoteStorageInfo;
  }

  @VisibleForTesting
  protected boolean hasErrorInStatusCheck() {
    return hasErrorInStatusCheck;
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
        if (appIdToRemoteStorageInfo.containsKey(appId)) {
          decRemoteStorageCounter(appIdToRemoteStorageInfo.get(appId).getPath());
          appIdToRemoteStorageInfo.remove(appId);
        }
      }
      CoordinatorMetrics.gaugeRunningAppNum.set(appIds.size());
      updateRemoteStorageMetrics();
    } catch (Exception e) {
      // the flag is only for test case
      hasErrorInStatusCheck = true;
      LOG.warn("Error happened in statusCheck", e);
    }
  }

  private void updateRemoteStorageMetrics() {
    for (String remoteStoragePath : availableRemoteStorageInfo.keySet()) {
      try {
        String storageHost = getStorageHost(remoteStoragePath);
        CoordinatorMetrics.updateDynamicGaugeForRemoteStorage(storageHost,
            remoteStoragePathCounter.get(remoteStoragePath).get());
      } catch (Exception e) {
        LOG.warn("Update remote storage metrics for {} failed ", remoteStoragePath);
      }
    }
  }

  private void addRemoteStorageMetrics(String remoteStoragePath) {
    String storageHost = getStorageHost(remoteStoragePath);
    if (!StringUtils.isEmpty(storageHost)) {
      CoordinatorMetrics.addDynamicGaugeForRemoteStorage(getStorageHost(remoteStoragePath));
      LOG.info("Add remote storage metrics for {} successfully ", remoteStoragePath);
    }
  }

  private String getStorageHost(String remoteStoragePath) {
    if (remoteStorageToHost.containsKey(remoteStoragePath)) {
      return remoteStorageToHost.get(remoteStoragePath);
    }
    String storageHost = "";
    try {
      URI uri = new URI(remoteStoragePath);
      storageHost = uri.getHost();
      remoteStorageToHost.put(remoteStoragePath, storageHost);
    } catch (URISyntaxException e) {
      LOG.warn("Invalid format of remoteStoragePath to get host, {}", remoteStoragePath);
    }
    return storageHost;
  }
}
