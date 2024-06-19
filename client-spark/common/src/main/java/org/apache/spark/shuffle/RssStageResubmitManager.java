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
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.stage.RssShuffleStatus;
import org.apache.spark.shuffle.stage.RssShuffleStatusForReader;
import org.apache.spark.shuffle.stage.RssShuffleStatusForWriter;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is to manage shuffle status for stage retry */
public class RssStageResubmitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RssStageResubmitManager.class);

  private final SparkConf sparkConf;
  private final Map<Integer, RssShuffleStatusForReader> shuffleStatusForReader =
      new ConcurrentHashMap<>();
  private final Map<Integer, RssShuffleStatusForWriter> shuffleStatusForWriter =
      new ConcurrentHashMap<>();
  private final Map<Integer, Object> shuffleLock = new ConcurrentHashMap<>();

  private final Set<String> blackListedServerIds = new ConcurrentHashSet<>();

  public RssStageResubmitManager(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  public Object getOrCreateShuffleLock(int shuffleId) {
    return shuffleLock.computeIfAbsent(shuffleId, x -> new Object());
  }

  public void clear(int shuffleId) {
    shuffleStatusForReader.remove(shuffleId);
    shuffleStatusForWriter.remove(shuffleId);
    shuffleLock.remove(shuffleId);
  }

  public boolean isStageAttemptRetried(int shuffleId, int stageId, int stageAttemptNumber) {
    RssShuffleStatus readerShuffleStatus = shuffleStatusForReader.get(shuffleId);
    RssShuffleStatus writerShuffleStatus = shuffleStatusForWriter.get(shuffleId);
    if (readerShuffleStatus == null && writerShuffleStatus == null) {
      return false;
    }
    if (readerShuffleStatus != null
        && readerShuffleStatus.isStageAttemptRetried(stageAttemptNumber)) {
      return true;
    }
    if (writerShuffleStatus != null
        && writerShuffleStatus.isStageAttemptRetried(stageAttemptNumber)) {
      return true;
    }
    return false;
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

  public boolean activateStageRetry(RssShuffleStatus shuffleStatus) {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    int sparkTaskMaxFailures = sparkConf.getInt(TASK_MAX_FAILURE, 4);
    if (shuffleStatus instanceof RssShuffleStatusForReader) {
      if (shuffleStatus.getStageRetriedCount() > 1) {
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

  public Set<String> getBlackListedServerIds() {
    return blackListedServerIds.stream().collect(Collectors.toSet());
  }

  public void addBlackListedServer(String shuffleServerId) {
    blackListedServerIds.add(shuffleServerId);
  }
}
