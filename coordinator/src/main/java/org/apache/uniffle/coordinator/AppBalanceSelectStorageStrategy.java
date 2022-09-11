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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.coordinator.LowestIOSampleCostSelectStorageStrategy.RankValue;

/**
 * AppBalanceSelectStorageStrategy will consider the number of apps allocated on each remote path is balanced.
 */
public class AppBalanceSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AppBalanceSelectStorageStrategy.class);
  /**
   * store remote path -> application count for assignment strategy
   */
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private FileSystem fs;

  public AppBalanceSelectStorageStrategy(Map<String, RankValue> remoteStoragePathRankValue) {
    this.remoteStoragePathRankValue = remoteStoragePathRankValue;
  }

  @VisibleForTesting
  @Override
  public List<Map.Entry<String, RankValue>> sortPathByRankValue(
      String path, Path testPath, long time, boolean isHealthy) {
    try {
      fs.delete(testPath, true);
      if (isHealthy) {
        RankValue rankValue = remoteStoragePathRankValue.get(path);
        remoteStoragePathRankValue.put(path, new RankValue(0, rankValue.getAppNum().get()));
      }
    } catch (Exception e) {
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      LOG.error("Failed to sort, we will not use this remote path {}.", path, e);
    }
    return Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream()
        .filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public void setFs(FileSystem fs) {
    this.fs = fs;
  }
}
