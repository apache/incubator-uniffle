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

package org.apache.spark.shuffle;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;

public class RssStageResubmitManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssStageResubmitManager.class);
  /** A list of shuffleServer for Write failures */
  private Set<String> failuresShuffleServerIds;
  /**
   * Prevent multiple tasks from reporting FetchFailed, resulting in multiple ShuffleServer
   * assignments, stageID, Attemptnumber Whether to reassign the combination flag;
   */
  private Map<Integer, RssStageResubmit> serverAssignedInfos;

  public RssStageResubmitManager() {
    this.failuresShuffleServerIds = Sets.newConcurrentHashSet();
    this.serverAssignedInfos = JavaUtils.newConcurrentMap();
  }

  public Set<String> getFailuresShuffleServerIds() {
    return failuresShuffleServerIds;
  }

  public void setFailuresShuffleServerIds(Set<String> failuresShuffleServerIds) {
    this.failuresShuffleServerIds = failuresShuffleServerIds;
  }

  public void recordFailuresShuffleServer(String shuffleServerId) {
    failuresShuffleServerIds.add(shuffleServerId);
  }

  public RssStageResubmit recordAndGetServerAssignedInfo(int shuffleId, String stageIdAndAttempt) {

    return serverAssignedInfos.computeIfAbsent(
        shuffleId, id -> new RssStageResubmit(stageIdAndAttempt, false));
  }

  public void recordAndGetServerAssignedInfo(
      int shuffleId, String stageIdAndAttempt, boolean isRetried) {
    serverAssignedInfos
        .computeIfAbsent(shuffleId, id -> new RssStageResubmit(stageIdAndAttempt, false))
        .setReassigned(isRetried);
  }
}
