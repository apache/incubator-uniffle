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

package org.apache.spark.shuffle.stage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * This class is to track the stage attempt status to check whether to trigger the stage retry of
 * Spark.
 */
public class RssShuffleStatus {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
  private final int stageId;
  private final int shuffleId;
  // the retried stage attempt records
  private final Set<Integer> stageAttemptRetriedRecords;

  private int stageAttemptNumber;
  // the failed task attempt numbers. Attention: these are not task attempt ids!
  private Set<Integer> taskAttemptFailureRecords;

  public RssShuffleStatus(int stageId, int shuffleId) {
    this.shuffleId = shuffleId;
    this.stageId = stageId;
    this.stageAttemptRetriedRecords = new HashSet<>();
    this.taskAttemptFailureRecords = new HashSet<>();
  }

  private <T> T withReadLock(Supplier<T> fn) {
    readLock.lock();
    try {
      return fn.get();
    } finally {
      readLock.unlock();
    }
  }

  private <T> T withWriteLock(Supplier<T> fn) {
    writeLock.lock();
    try {
      return fn.get();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isStageAttemptRetried(int stageAttempt) {
    return withReadLock(() -> stageAttemptRetriedRecords.contains(stageAttempt));
  }

  public int getStageRetriedCount() {
    return withReadLock(() -> this.stageAttemptRetriedRecords.size());
  }

  public void markStageAttemptRetried() {
    withWriteLock(
        () -> {
          this.stageAttemptRetriedRecords.add(stageAttemptNumber);
          return null;
        });
  }

  public int getStageAttempt() {
    return withReadLock(() -> this.stageAttemptNumber);
  }

  public boolean updateStageAttemptIfNecessary(int stageAttempt) {
    return withWriteLock(
        () -> {
          if (this.stageAttemptNumber < stageAttempt) {
            // a new stage attempt is issued.
            this.stageAttemptNumber = stageAttempt;
            this.taskAttemptFailureRecords = new HashSet<>();
            return true;
          } else if (this.stageAttemptNumber > stageAttempt) {
            return false;
          }
          return true;
        });
  }

  public void incTaskFailure(int taskAttemptNumber) {
    withWriteLock(
        () -> {
          taskAttemptFailureRecords.add(taskAttemptNumber);
          return null;
        });
  }

  public int getTaskFailureAttemptCount() {
    return withReadLock(() -> taskAttemptFailureRecords.size());
  }

  public int getStageId() {
    return stageId;
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
