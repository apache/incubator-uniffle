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

package org.apache.uniffle.shuffle.manager;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleManagerGrpc.ShuffleManagerImplBase;

public class ShuffleManagerGrpcService extends ShuffleManagerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcService.class);
  private final Map<Integer, RssShuffleStatus> shuffleStatus = JavaUtils.newConcurrentMap();
  private final RssShuffleManagerInterface shuffleManager;

  public ShuffleManagerGrpcService(RssShuffleManagerInterface shuffleManager) {
    this.shuffleManager = shuffleManager;
  }

  @Override
  public void reportShuffleFetchFailure(RssProtos.ReportShuffleFetchFailureRequest request,
      StreamObserver<RssProtos.ReportShuffleFetchFailureResponse> responseObserver) {
    String appId = request.getAppId();
    int stageAttempt = request.getStageAttemptId();
    int partitionId = request.getPartitionId();
    RssProtos.StatusCode code;
    boolean reSubmitWholeStage;
    String msg;
    if (!appId.equals(shuffleManager.getAppId())) {
      msg = String.format("got a wrong shuffle fetch failure report from appId: %s, expected appId: %s",
          appId, shuffleManager.getAppId());
      LOG.warn(msg);
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reSubmitWholeStage = false;
    } else {
      RssShuffleStatus status = shuffleStatus.computeIfAbsent(request.getShuffleId(), key -> {
            int partitionNum = shuffleManager.getPartitionNum(key);
            return new RssShuffleStatus(partitionNum, stageAttempt);
          }
      );
      int c = status.resetStageAttemptIfNecessary(stageAttempt);
      if (c < 0) {
        msg = String.format("got an old stage(%d vs %d) shuffle fetch failure report, which should be impossible.",
            status.getStageAttempt(), stageAttempt);
        LOG.warn(msg);
        code = RssProtos.StatusCode.INVALID_REQUEST;
        reSubmitWholeStage = false;
      } else { // update the stage partition fetch failure count
        code = RssProtos.StatusCode.SUCCESS;
        status.incPartitionFetchFailure(stageAttempt, partitionId);
        int fetchFailureNum = status.getPartitionFetchFailureNum(stageAttempt, partitionId);
        if (fetchFailureNum >= shuffleManager.getMaxFetchFailures()) {
          reSubmitWholeStage = true;
          msg = String.format("report shuffle fetch failure as maximum number(%d) of shuffle fetch is occurred",
              shuffleManager.getMaxFetchFailures());
        } else {
          reSubmitWholeStage = false;
          msg = "don't report shuffle fetch failure";
        }
      }
    }

    RssProtos.ReportShuffleFetchFailureResponse reply = RssProtos.ReportShuffleFetchFailureResponse
        .newBuilder()
        .setStatus(code)
        .setReSubmitWholeStage(reSubmitWholeStage)
        .setMsg(msg)
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * Remove the no longer used shuffle id's rss shuffle status. This is called when ShuffleManager unregisters the
   * corresponding shuffle id.
   * @param shuffleId the shuffle id to unregister.
   */
  public void unregisterShuffle(int shuffleId) {
    shuffleStatus.remove(shuffleId);
  }

  private static class RssShuffleStatus {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final int[] partitions;
    private int stageAttempt;

    private RssShuffleStatus(int partitionNum, int stageAttempt) {
      this.stageAttempt = stageAttempt;
      this.partitions = new int[partitionNum];
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

    // todo: maybe it's more performant to just use synchronized method here.
    public int getStageAttempt() {
      return withReadLock(() -> this.stageAttempt);
    }

    /**
     * Check whether the input stage attempt is a new stage or not. If a new stage attempt is requested, reset
     * partitions.
     * @param stageAttempt the incoming stage attempt number
     * @return 0 if stageAttempt == this.stageAttempt
     *         1 if stageAttempt > this.stageAttempt
     *         -1 if stateAttempt < this.stageAttempt which means nothing happens
     */
    public int resetStageAttemptIfNecessary(int stageAttempt) {
      return withWriteLock(() -> {
        if (this.stageAttempt < stageAttempt) {
          // a new stage attempt is issued. the partitions array should be clear and reset.
          Arrays.fill(this.partitions, 0);
          this.stageAttempt = stageAttempt;
          return 1;
        } else if (this.stageAttempt > stageAttempt) {
          return -1;
        }
        return 0;
      });
    }

    public void incPartitionFetchFailure(int stageAttempt, int partition) {
      withWriteLock(() -> {
        if (this.stageAttempt != stageAttempt) {
          // do nothing here
        } else {
          this.partitions[partition] = this.partitions[partition] + 1;
        }
        return null;
      });
    }

    public int getPartitionFetchFailureNum(int stageAttempt, int partition) {
      return withReadLock(() -> {
        if (this.stageAttempt != stageAttempt) {
          return 0;
        } else {
          return this.partitions[partition];
        }
      });
    }
  }
}
