/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
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

import java.io.File;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.storage.util.ShuffleStorageUtils;

public class LocalStorageChecker extends Checker {

  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageChecker.class);

  private final double diskMaxUsagePercentage;
  private final double diskRecoveryUsagePercentage;
  private final double minStorageHealthyPercentage;
  private final List<StorageInfo> storageInfos  = Lists.newArrayList();
  private boolean isHealthy = true;

  public LocalStorageChecker(ShuffleServerConf conf) {
    super(conf);
    String basePathStr = conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH);
    if (StringUtils.isEmpty(basePathStr)) {
      throw new IllegalArgumentException("The base path cannot be empty");
    }
    String storageType = conf.getString(ShuffleServerConf.RSS_STORAGE_TYPE);
    if (!ShuffleStorageUtils.containsLocalFile(storageType)) {
      throw new IllegalArgumentException("Only StorageType contains LOCALFILE support storageChecker");
    }
    String[] storagePaths = basePathStr.split(",");

    for (String path : storagePaths) {
      storageInfos.add(new StorageInfo(path));
    }
    this.diskMaxUsagePercentage = conf.getDouble(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE);
    this.diskRecoveryUsagePercentage = conf.getDouble(ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE);
    this.minStorageHealthyPercentage = conf.getDouble(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE);
  }

  @Override
  public boolean checkIsHealthy() {
    int num = 0;
    for (StorageInfo storageInfo : storageInfos) {
      if (storageInfo.checkIsHealthy()) {
        num++;
      }
    }

    if (storageInfos.isEmpty()) {
      if (isHealthy) {
        LOG.info("shuffle server become unhealthy because of empty storage");
      }
      isHealthy = false;
      return false;
    }

    double availablePercentage = num * 100.0 / storageInfos.size();
    if (Double.compare(availablePercentage, minStorageHealthyPercentage) >= 0) {
      if (!isHealthy) {
        LOG.info("shuffle server become healthy");
      }
      isHealthy = true;
    } else {
      if (isHealthy) {
        LOG.info("shuffle server become unhealthy");
      }
      isHealthy = false;
    }
    return isHealthy;
  }

  // Only for testing
  @VisibleForTesting
  long getTotalSpace(File file) {
    return file.getTotalSpace();
  }

  // Only for testing
  @VisibleForTesting
  long getUsedSpace(File file) {
    return file.getTotalSpace() - file.getUsableSpace();
  }

  // todo: This function will be integrated to MultiStorageManager, currently we only support disk check.
  class StorageInfo {

    private final File storageDir;
    private boolean isHealthy;

    StorageInfo(String path) {
      this.storageDir = new File(path);
      this.isHealthy = true;
    }

    boolean checkIsHealthy() {
      if (Double.compare(0.0, getTotalSpace(storageDir)) == 0) {
        this.isHealthy = false;
        return false;
      }
      double usagePercent = getUsedSpace(storageDir) * 100.0 / getTotalSpace(storageDir);
      if (isHealthy) {
        if (Double.compare(usagePercent, diskMaxUsagePercentage) >= 0) {
          isHealthy = false;
          LOG.info("storage {} become unhealthy", storageDir.getAbsolutePath());
        }
      } else {
        if (Double.compare(usagePercent, diskRecoveryUsagePercentage) <= 0) {
          isHealthy = true;
          LOG.info("storage {} become healthy", storageDir.getAbsolutePath());
        }
      }
      return isHealthy;
    }
  }
}
