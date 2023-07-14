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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.coordinator.CoordinatorConf;

/**
 * LowestIOSampleCostSelectStorageStrategy considers that when allocating apps to different remote
 * paths, remote paths that can write and read. Therefore, it may occur that all apps are written to
 * the same cluster. At the same time, if a cluster has read and write exceptions, we will
 * automatically avoid the cluster.
 */
public class LowestIOSampleCostSelectStorageStrategy extends AbstractSelectStorageStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(LowestIOSampleCostSelectStorageStrategy.class);
  /** store remote path -> application count for assignment strategy */
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;

  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;

  public LowestIOSampleCostSelectStorageStrategy(
      Map<String, RankValue> remoteStoragePathRankValue,
      Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo,
      Map<String, RemoteStorageInfo> availableRemoteStorageInfo,
      CoordinatorConf conf) {
    super(remoteStoragePathRankValue, conf);
    this.appIdToRemoteStorageInfo = appIdToRemoteStorageInfo;
    this.availableRemoteStorageInfo = availableRemoteStorageInfo;
  }

  @Override
  public Comparator<Map.Entry<String, RankValue>> getComparator() {
    return (x, y) -> {
      final long xReadAndWriteTime = x.getValue().getCostTime().get();
      final long yReadAndWriteTime = y.getValue().getCostTime().get();
      if (xReadAndWriteTime > yReadAndWriteTime) {
        return 1;
      } else if (xReadAndWriteTime < yReadAndWriteTime) {
        return -1;
      } else {
        return Integer.compare(x.getValue().getAppNum().get(), y.getValue().getAppNum().get());
      }
    };
  }

  @Override
  public synchronized RemoteStorageInfo pickStorage(String appId) {
    LOG.info("The sorted remote path list is: {}", uris);
    for (Map.Entry<String, RankValue> uri : uris) {
      String storagePath = uri.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        return appIdToRemoteStorageInfo.computeIfAbsent(
            appId, x -> availableRemoteStorageInfo.get(storagePath));
      }
    }
    LOG.warn("No remote storage is available, we will default to the first.");
    return availableRemoteStorageInfo.values().iterator().next();
  }

  @Override
  public int readAndWriteTimes(CoordinatorConf conf) {
    return conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_ACCESS_TIMES);
  }
}
