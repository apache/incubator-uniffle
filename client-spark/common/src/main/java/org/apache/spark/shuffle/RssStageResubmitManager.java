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
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.stage.RssShuffleStatus;
import org.apache.spark.shuffle.stage.RssShuffleStatusForReader;
import org.apache.spark.shuffle.stage.RssShuffleStatusForWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;

public class RssStageResubmitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RssStageResubmitManager.class);

  private final SparkConf sparkConf = new SparkConf();
  private final Map<Integer, RssShuffleStatusForReader> shuffleStatusForReader =
      new ConcurrentHashMap<>();
  private final Map<Integer, RssShuffleStatusForWriter> shuffleStatusForWriter =
      new ConcurrentHashMap<>();

  public void clear(int shuffleId) {
    shuffleStatusForReader.remove(shuffleId);
    shuffleStatusForWriter.remove(shuffleId);
  }

  public RssShuffleStatus getShuffleStatusForReader(int shuffleId, int stageId, int stageAttempt) {
    RssShuffleStatus shuffleStatus =
        shuffleStatusForReader.computeIfAbsent(
            shuffleId, x -> new RssShuffleStatusForReader(stageId, shuffleId));
    if (shuffleStatus.updateStageAttemptIfNecessary(stageAttempt)) {
      return shuffleStatus;
    }
    return null;
  }

  public RssShuffleStatus getShuffleStatusForWriter(int shuffleId, int stageId, int stageAttempt) {
    RssShuffleStatus shuffleStatus =
        shuffleStatusForWriter.computeIfAbsent(
            shuffleId, x -> new RssShuffleStatusForWriter(stageId, shuffleId));
    if (shuffleStatus.updateStageAttemptIfNecessary(stageAttempt)) {
      return shuffleStatus;
    }
    return null;
  }

  public boolean triggerStageRetry(RssShuffleStatus shuffleStatus) {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    int sparkTaskMaxFailures = sparkConf.getInt(TASK_MAX_FAILURE, 4);
    if (shuffleStatus instanceof RssShuffleStatusForReader) {
      if (shuffleStatus.getStageRetriedNumber() > 1) {
        LOG.warn("The shuffleId:{}, stageId:{} has been retried. Ignore it.");
        return false;
      }
      if (shuffleStatus.getTaskFailureAttemptCount() >= sparkTaskMaxFailures) {
        shuffleStatus.markStageAttemptRetried();
        return true;
      }
    }
    return false;
  }

  /** Blacklist of the Shuffle Server when the write fails. */
  private Set<String> serverIdBlackList;
  /**
   * Prevent multiple tasks from reporting FetchFailed, resulting in multiple ShuffleServer
   * assignments, stageID, Attemptnumber Whether to reassign the combination flag;
   */
  private Map<Integer, RssStageInfo> serverAssignedInfos;

  public RssStageResubmitManager() {
    this.serverIdBlackList = Sets.newConcurrentHashSet();
    this.serverAssignedInfos = JavaUtils.newConcurrentMap();
  }

  public Set<String> getServerIdBlackList() {
    return serverIdBlackList;
  }

  public void resetServerIdBlackList(Set<String> failuresShuffleServerIds) {
    this.serverIdBlackList = failuresShuffleServerIds;
  }

  public void recordFailuresShuffleServer(String shuffleServerId) {
    serverIdBlackList.add(shuffleServerId);
  }

  public RssStageInfo recordAndGetServerAssignedInfo(int shuffleId, String stageIdAndAttempt) {

    return serverAssignedInfos.computeIfAbsent(
        shuffleId, id -> new RssStageInfo(stageIdAndAttempt, false));
  }

  public void recordAndGetServerAssignedInfo(
      int shuffleId, String stageIdAndAttempt, boolean isRetried) {
    serverAssignedInfos
        .computeIfAbsent(shuffleId, id -> new RssStageInfo(stageIdAndAttempt, false))
        .setReassigned(isRetried);
  }
}
