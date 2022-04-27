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

package org.apache.spark.shuffle;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.spark.ShuffleDependency;

import com.tencent.rss.common.ShuffleServerInfo;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

  private String appId;
  private int numMaps;
  private ShuffleDependency<K, V, C> dependency;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  // shuffle servers which is for store shuffle data
  private Set<ShuffleServerInfo> shuffleServersForData;
  // remoteStorage used for this job
  private String remoteStorage;

  public RssShuffleHandle(
      int shuffleId,
      String appId,
      int numMaps,
      ShuffleDependency<K, V, C> dependency,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      String remoteStorage) {
    super(shuffleId);
    this.appId = appId;
    this.numMaps = numMaps;
    this.dependency = dependency;
    this.partitionToServers = partitionToServers;
    this.remoteStorage = remoteStorage;
    shuffleServersForData = Sets.newHashSet();
    for (List<ShuffleServerInfo> ssis : partitionToServers.values()) {
      shuffleServersForData.addAll(ssis);
    }
  }

  public String getAppId() {
    return appId;
  }

  public int getNumMaps() {
    return numMaps;
  }

  public ShuffleDependency<K, V, C> getDependency() {
    return dependency;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public int getShuffleId() {
    return shuffleId();
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return shuffleServersForData;
  }

  public String getRemoteStorage() {
    return remoteStorage;
  }
}
