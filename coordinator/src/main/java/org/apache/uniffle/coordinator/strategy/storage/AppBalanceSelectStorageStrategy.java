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

package org.apache.uniffle.coordinator.strategy.storage;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.coordinator.CoordinatorConf;

/**
 * AppBalanceSelectStorageStrategy will consider the number of apps allocated on each remote path is
 * balanced.
 */
public class AppBalanceSelectStorageStrategy extends AbstractSelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AppBalanceSelectStorageStrategy.class);
  /** store remote path -> application count for assignment strategy */
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;

  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;

  public AppBalanceSelectStorageStrategy(
      Map<String, RankValue> remoteStoragePathRankValue,
      Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo,
      Map<String, RemoteStorageInfo> availableRemoteStorageInfo,
      CoordinatorConf conf) {
    super(remoteStoragePathRankValue, conf);
    this.appIdToRemoteStorageInfo = appIdToRemoteStorageInfo;
    this.availableRemoteStorageInfo = availableRemoteStorageInfo;
  }

  /**
   * When choosing the AppBalance strategy, each time you select a path, you should know the number
   * of the latest apps in different paths
   */
  @Override
  public synchronized RemoteStorageInfo pickStorage(String appId) {
    boolean isUnhealthy =
        uris.stream().noneMatch(rv -> rv.getValue().getCostTime().get() != Long.MAX_VALUE);
    if (!isUnhealthy) {
      // If there is only one unhealthy path, then filter that path
      uris =
          uris.stream()
              .filter(rv -> rv.getValue().getCostTime().get() != Long.MAX_VALUE)
              .sorted(Comparator.comparingInt(entry -> entry.getValue().getAppNum().get()))
              .collect(Collectors.toList());
    } else {
      // If all paths are unhealthy, assign paths according to the number of apps
      uris =
          uris.stream()
              .sorted(Comparator.comparingInt(entry -> entry.getValue().getAppNum().get()))
              .collect(Collectors.toList());
    }
    LOG.info("The sorted remote path list is: {}", uris);
    for (Map.Entry<String, RankValue> entry : uris) {
      String storagePath = entry.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        return appIdToRemoteStorageInfo.computeIfAbsent(
            appId, x -> availableRemoteStorageInfo.get(storagePath));
      }
    }
    LOG.warn("No remote storage is available, we will default to the first.");
    return availableRemoteStorageInfo.values().iterator().next();
  }
}
