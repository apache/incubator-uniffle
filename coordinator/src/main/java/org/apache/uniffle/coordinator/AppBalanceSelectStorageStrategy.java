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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.coordinator.LowestIOSampleCostSelectStorageStrategy.RankValue;

/**
 * AppBalanceSelectStorageStrategy will consider the number of apps allocated on each remote path is balanced.
 */
public class AppBalanceSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AppBalanceSelectStorageStrategy.class);
  /**
   * store appId -> remote path to make sure all shuffle data of the same application
   * will be written to the same remote storage
   */
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;
  /**
   * store remote path -> application count for assignment strategy
   */
  private final Map<String, RankValue> remoteStoragePathCounter;
  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;

  public AppBalanceSelectStorageStrategy() {
    this.appIdToRemoteStorageInfo = Maps.newConcurrentMap();
    this.remoteStoragePathCounter = Maps.newConcurrentMap();
    this.availableRemoteStorageInfo = Maps.newHashMap();
  }

  /**
   * the strategy of pick remote storage is according to assignment count
   */
  @Override
  public RemoteStorageInfo pickRemoteStorage(String appId) {
    if (appIdToRemoteStorageInfo.containsKey(appId)) {
      return appIdToRemoteStorageInfo.get(appId);
    }

    // create list for sort
    List<Map.Entry<String, RankValue>> sizeList =
        Lists.newArrayList(remoteStoragePathCounter.entrySet()).stream().filter(Objects::nonNull)
            .sorted(Comparator.comparingInt(entry -> entry.getValue().getAppNum().get())).collect(Collectors.toList());

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

  @Override
  @VisibleForTesting
  public synchronized void incRemoteStorageCounter(String remoteStoragePath) {
    RankValue counter = remoteStoragePathCounter.get(remoteStoragePath);
    if (counter != null) {
      counter.getAppNum().incrementAndGet();
    } else {
      // it may be happened when assignment remote storage
      // and refresh remote storage at the same time
      LOG.warn("Remote storage path lost during assignment: {} doesn't exist, reset it to 1",
          remoteStoragePath);
      remoteStoragePathCounter.put(remoteStoragePath, new RankValue(1));
    }
  }

  @Override
  @VisibleForTesting
  public synchronized void decRemoteStorageCounter(String storagePath) {
    if (!StringUtils.isEmpty(storagePath)) {
      RankValue atomic = remoteStoragePathCounter.get(storagePath);
      if (atomic != null) {
        double count = atomic.getAppNum().decrementAndGet();
        if (count < 0) {
          LOG.warn("Unexpected counter for remote storage: {}, which is {}, reset to 0",
              storagePath, count);
          atomic.getAppNum().set(0);
        }
      } else {
        LOG.warn("Can't find counter for remote storage: {}", storagePath);
        remoteStoragePathCounter.putIfAbsent(storagePath, new RankValue(0));
      }
      if (remoteStoragePathCounter.get(storagePath).getAppNum().get() == 0
          && !availableRemoteStorageInfo.containsKey(storagePath)) {
        remoteStoragePathCounter.remove(storagePath);
      }
    }
  }

  @Override
  public synchronized void removePathFromCounter(String storagePath) {
    RankValue atomic = remoteStoragePathCounter.get(storagePath);
    if (atomic != null && atomic.getAppNum().get() == 0) {
      remoteStoragePathCounter.remove(storagePath);
    }
  }

  @Override
  public Map<String, RemoteStorageInfo> getAppIdToRemoteStorageInfo() {
    return appIdToRemoteStorageInfo;
  }

  @Override
  public Map<String, RankValue> getRemoteStoragePathRankValue() {
    return remoteStoragePathCounter;
  }

  @Override
  public Map<String, RemoteStorageInfo> getAvailableRemoteStorageInfo() {
    return availableRemoteStorageInfo;
  }
}
