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

package org.apache.uniffle.server;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Maps;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleDataDistributionType;

/**
 * ShuffleTaskInfo contains the information of submitting the shuffle,
 * the information of the cache block, user and timestamp corresponding to the app
 */
public class ShuffleTaskInfo {

  private Long currentTimes;
  /**
   * shuffleId -> commit count
   */
  private Map<Integer, AtomicInteger> commitCounts;
  private Map<Integer, Object> commitLocks;
  /**
   * shuffleId -> blockIds
    */
  private Map<Integer, Roaring64NavigableMap> cachedBlockIds;
  private AtomicReference<String> user;

  private AtomicReference<ShuffleDataDistributionType> dataDistType;

  public ShuffleTaskInfo() {
    this.currentTimes = System.currentTimeMillis();
    this.commitCounts = Maps.newConcurrentMap();
    this.commitLocks = Maps.newConcurrentMap();
    this.cachedBlockIds = Maps.newConcurrentMap();
    this.user = new AtomicReference<>();
    this.dataDistType = new AtomicReference<>();
  }

  public Long getCurrentTimes() {
    return currentTimes;
  }

  public void setCurrentTimes(Long currentTimes) {
    this.currentTimes = currentTimes;
  }

  public Map<Integer, AtomicInteger> getCommitCounts() {
    return commitCounts;
  }

  public Map<Integer, Object> getCommitLocks() {
    return commitLocks;
  }

  public Map<Integer, Roaring64NavigableMap> getCachedBlockIds() {
    return cachedBlockIds;
  }

  public String getUser() {
    return user.get();
  }

  public void setUser(String user) {
    this.user.set(user);
  }

  public void setDataDistType(
      ShuffleDataDistributionType dataDistType) {
    this.dataDistType.set(dataDistType);
  }

  public ShuffleDataDistributionType getDataDistType() {
    return dataDistType.get();
  }
}
