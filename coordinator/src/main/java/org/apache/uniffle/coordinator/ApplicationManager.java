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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.uniffle.coordinator.LowestIOSampleCostSelectStorageStrategy.RankValue;

public class ApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  // TODO: Add anomaly detection for other storage
  public static final List<String> REMOTE_PATH_SCHEMA = Arrays.asList("hdfs");
  private final long expired;
  private final StrategyName storageStrategy;
  private final Map<String, Long> appIds = Maps.newConcurrentMap();
  private final SelectStorageStrategy selectStorageStrategy;
  // store appId -> remote path to make sure all shuffle data of the same application
  // will be written to the same remote storage
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;
  // store remote path -> application count for assignment strategy
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private final Map<String, String> remoteStorageToHost = Maps.newConcurrentMap();
  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;
  private List<Map.Entry<String, RankValue>> sizeList;
  // it's only for test case to check if status check has problem
  private boolean hasErrorInStatusCheck = false;

  public ApplicationManager(CoordinatorConf conf) {
    storageStrategy = conf.get(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SELECT_STRATEGY);
    appIdToRemoteStorageInfo = Maps.newConcurrentMap();
    remoteStoragePathRankValue = Maps.newConcurrentMap();
    availableRemoteStorageInfo = Maps.newConcurrentMap();
    if (StrategyName.IO_SAMPLE == storageStrategy) {
      selectStorageStrategy = new LowestIOSampleCostSelectStorageStrategy(remoteStoragePathRankValue, conf);
    } else if (StrategyName.APP_BALANCE == storageStrategy) {
      selectStorageStrategy = new AppBalanceSelectStorageStrategy(remoteStoragePathRankValue, conf);
    } else {
      throw new UnsupportedOperationException("Unsupported selected storage strategy.");
    }
    expired = conf.getLong(CoordinatorConf.COORDINATOR_APP_EXPIRED);
    // the thread for checking application status
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("ApplicationManager-%d"));
    scheduledExecutorService.scheduleAtFixedRate(
        this::statusCheck, expired / 2, expired / 2, TimeUnit.MILLISECONDS);
    // the thread for checking if the hdfs path is readable and writable
    ScheduledExecutorService readWriteRankScheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("readWriteRankScheduler-%d"));
    // should init later than the refreshRemoteStorage init
    readWriteRankScheduler.scheduleAtFixedRate(this::checkReadAndWrite, 1000,
        conf.getLong(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_TIME), TimeUnit.MILLISECONDS);
  }

  public void checkReadAndWrite() {
    if (remoteStoragePathRankValue.size() > 1) {
      for (String path : remoteStoragePathRankValue.keySet()) {
        sizeList = selectStorageStrategy.readAndWrite(path);
      }
    } else {
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
    }
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
          remoteStoragePathRankValue.putIfAbsent(path, new RankValue(0));
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
    // If the APP_BALANCE strategy is used, we must ensure that each allocation is uniform
    useAppBalanceToSelect();
    for (Map.Entry<String, RankValue> entry : sizeList) {
      String storagePath = entry.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        appIdToRemoteStorageInfo.putIfAbsent(appId, availableRemoteStorageInfo.get(storagePath));
        incRemoteStorageCounter(storagePath);
        break;
      }
    }
    return appIdToRemoteStorageInfo.get(appId);
  }

  /**
   * When choosing the AppBalance strategy, each time you select a path,
   * you should know the number of the latest apps in different paths
   */
  @VisibleForTesting
  public void useAppBalanceToSelect() {
    boolean isAppBalance = storageStrategy.equals(StrategyName.APP_BALANCE);
    boolean isUnhealthy =
        sizeList.stream().noneMatch(rv -> rv.getValue().getReadAndWriteTime().get() != Long.MAX_VALUE);
    if (isAppBalance) {
      if (!isUnhealthy) {
        // If there is only one unhealthy path, then filter that path
        sizeList = sizeList.stream().filter(rv -> rv.getValue().getReadAndWriteTime().get() != Long.MAX_VALUE).sorted(
            Comparator.comparingInt(entry -> entry.getValue().getAppNum().get())).collect(Collectors.toList());
      } else {
        // If all paths are unhealthy, assign paths according to the number of apps
        sizeList = sizeList.stream().sorted(Comparator.comparingInt(
            entry -> entry.getValue().getAppNum().get())).collect(Collectors.toList());
      }
    }
    LOG.error("The sorted remote path list is: {}", sizeList);
  }

  @VisibleForTesting
  public synchronized void incRemoteStorageCounter(String remoteStoragePath) {
    RankValue counter = remoteStoragePathRankValue.get(remoteStoragePath);
    if (counter != null) {
      counter.getAppNum().incrementAndGet();
    } else {
      // it may be happened when assignment remote storage
      // and refresh remote storage at the same time
      LOG.warn("Remote storage path lost during assignment: {} doesn't exist, reset it to 1",
          remoteStoragePath);
      remoteStoragePathRankValue.put(remoteStoragePath, new RankValue(1));
    }
  }

  @VisibleForTesting
  public synchronized void decRemoteStorageCounter(String storagePath) {
    if (!StringUtils.isEmpty(storagePath)) {
      RankValue atomic = remoteStoragePathRankValue.get(storagePath);
      if (atomic != null) {
        double count = atomic.getAppNum().decrementAndGet();
        if (count < 0) {
          LOG.warn("Unexpected counter for remote storage: {}, which is {}, reset to 0",
              storagePath, count);
          atomic.getAppNum().set(0);
        }
      } else {
        LOG.warn("Can't find counter for remote storage: {}", storagePath);
        remoteStoragePathRankValue.putIfAbsent(storagePath, new RankValue(0));
      }
      if (remoteStoragePathRankValue.get(storagePath).getAppNum().get() == 0
          && !availableRemoteStorageInfo.containsKey(storagePath)) {
        remoteStoragePathRankValue.remove(storagePath);
      }
    }
  }

  public synchronized void removePathFromCounter(String storagePath) {
    RankValue atomic = remoteStoragePathRankValue.get(storagePath);
    // The time spent reading and writing cannot be used to determine whether the current path is still used by apps.
    // Therefore, determine whether the HDFS path is still used by the number of apps
    if (atomic != null && atomic.getAppNum().get() == 0) {
      remoteStoragePathRankValue.remove(storagePath);
    }
  }

  public Set<String> getAppIds() {
    return appIds.keySet();
  }

  @VisibleForTesting
  protected Map<String, RankValue> getRemoteStoragePathRankValue() {
    return remoteStoragePathRankValue;
  }

  @VisibleForTesting
  public SelectStorageStrategy getSelectStorageStrategy() {
    return selectStorageStrategy;
  }

  @VisibleForTesting
  public Map<String, RemoteStorageInfo> getAvailableRemoteStorageInfo() {
    return availableRemoteStorageInfo;
  }

  @VisibleForTesting
  public Map<String, RemoteStorageInfo> getAppIdToRemoteStorageInfo() {
    return appIdToRemoteStorageInfo;
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
            remoteStoragePathRankValue.get(remoteStoragePath).getAppNum().get());
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

  public enum StrategyName {
    APP_BALANCE,
    IO_SAMPLE
  }
}
