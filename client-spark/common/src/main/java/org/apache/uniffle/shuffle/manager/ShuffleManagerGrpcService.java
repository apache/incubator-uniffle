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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleManagerGrpc.ShuffleManagerImplBase;
import org.apache.uniffle.shuffle.BlockIdManager;

public class ShuffleManagerGrpcService extends ShuffleManagerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcService.class);
  private final Map<Integer, RssShuffleStatus> shuffleStatus = JavaUtils.newConcurrentMap();
  // The shuffleId mapping records the number of ShuffleServer write failures
  private final Map<Integer, ShuffleServerFailureRecord> shuffleWrtieStatus =
      JavaUtils.newConcurrentMap();
  private final RssShuffleManagerInterface shuffleManager;

  public ShuffleManagerGrpcService(RssShuffleManagerInterface shuffleManager) {
    this.shuffleManager = shuffleManager;
  }

  @Override
  public void reportShuffleWriteFailure(
      RssProtos.ReportShuffleWriteFailureRequest request,
      StreamObserver<RssProtos.ReportShuffleWriteFailureResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int stageAttemptNumber = request.getStageAttemptNumber();
    List<RssProtos.ShuffleServerId> shuffleServerIdsList = request.getShuffleServerIdsList();
    RssProtos.StatusCode code;
    boolean reSubmitWholeStage;
    String msg;
    if (!appId.equals(shuffleManager.getAppId())) {
      msg =
          String.format(
              "got a wrong shuffle write failure report from appId: %s, expected appId: %s",
              appId, shuffleManager.getAppId());
      LOG.warn(msg);
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reSubmitWholeStage = false;
    } else {
      Map<String, AtomicInteger> shuffleServerInfoIntegerMap = JavaUtils.newConcurrentMap();
      List<ShuffleServerInfo> shuffleServerInfos =
          ShuffleServerInfo.fromProto(shuffleServerIdsList);
      shuffleServerInfos.forEach(
          shuffleServerInfo -> {
            shuffleServerInfoIntegerMap.put(shuffleServerInfo.getId(), new AtomicInteger(0));
          });
      ShuffleServerFailureRecord shuffleServerFailureRecord =
          shuffleWrtieStatus.computeIfAbsent(
              shuffleId,
              key ->
                  new ShuffleServerFailureRecord(shuffleServerInfoIntegerMap, stageAttemptNumber));
      boolean resetflag =
          shuffleServerFailureRecord.resetStageAttemptIfNecessary(stageAttemptNumber);
      if (resetflag) {
        msg =
            String.format(
                "got an old stage(%d vs %d) shuffle write failure report, which should be impossible.",
                shuffleServerFailureRecord.getStageAttempt(), stageAttemptNumber);
        LOG.warn(msg);
        code = RssProtos.StatusCode.INVALID_REQUEST;
        reSubmitWholeStage = false;
      } else {
        code = RssProtos.StatusCode.SUCCESS;
        // update the stage shuffleServer write failed count
        boolean fetchFailureflag =
            shuffleServerFailureRecord.incPartitionWriteFailure(
                stageAttemptNumber, shuffleServerInfos, shuffleManager);
        if (fetchFailureflag) {
          reSubmitWholeStage = true;
          msg =
              String.format(
                  "report shuffle write failure as maximum number(%d) of shuffle write is occurred",
                  shuffleManager.getMaxFetchFailures());
        } else {
          reSubmitWholeStage = false;
          msg = "don't report shuffle write failure";
        }
      }
    }

    RssProtos.ReportShuffleWriteFailureResponse reply =
        RssProtos.ReportShuffleWriteFailureResponse.newBuilder()
            .setStatus(code)
            .setReSubmitWholeStage(reSubmitWholeStage)
            .setMsg(msg)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleFetchFailure(
      RssProtos.ReportShuffleFetchFailureRequest request,
      StreamObserver<RssProtos.ReportShuffleFetchFailureResponse> responseObserver) {
    String appId = request.getAppId();
    int stageAttempt = request.getStageAttemptId();
    int partitionId = request.getPartitionId();
    RssProtos.StatusCode code;
    boolean reSubmitWholeStage;
    String msg;
    if (!appId.equals(shuffleManager.getAppId())) {
      msg =
          String.format(
              "got a wrong shuffle fetch failure report from appId: %s, expected appId: %s",
              appId, shuffleManager.getAppId());
      LOG.warn(msg);
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reSubmitWholeStage = false;
    } else {
      RssShuffleStatus status =
          shuffleStatus.computeIfAbsent(
              request.getShuffleId(),
              key -> {
                int partitionNum = shuffleManager.getPartitionNum(key);
                return new RssShuffleStatus(partitionNum, stageAttempt);
              });
      int c = status.resetStageAttemptIfNecessary(stageAttempt);
      if (c < 0) {
        msg =
            String.format(
                "got an old stage(%d vs %d) shuffle fetch failure report, which should be impossible.",
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
          msg =
              String.format(
                  "report shuffle fetch failure as maximum number(%d) of shuffle fetch is occurred",
                  shuffleManager.getMaxFetchFailures());
        } else {
          reSubmitWholeStage = false;
          msg = "don't report shuffle fetch failure";
        }
      }
    }

    RssProtos.ReportShuffleFetchFailureResponse reply =
        RssProtos.ReportShuffleFetchFailureResponse.newBuilder()
            .setStatus(code)
            .setReSubmitWholeStage(reSubmitWholeStage)
            .setMsg(msg)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getPartitionToShufflerServerWithStageRetry(
      RssProtos.PartitionToShuffleServerRequest request,
      StreamObserver<RssProtos.ReassignOnStageRetryResponse> responseObserver) {
    RssProtos.ReassignOnStageRetryResponse reply;
    RssProtos.StatusCode code;
    int shuffleId = request.getShuffleId();
    StageAttemptShuffleHandleInfo shuffleHandle =
        (StageAttemptShuffleHandleInfo) shuffleManager.getShuffleHandleInfoByShuffleId(shuffleId);
    if (shuffleHandle != null) {
      code = RssProtos.StatusCode.SUCCESS;
      reply =
          RssProtos.ReassignOnStageRetryResponse.newBuilder()
              .setStatus(code)
              .setShuffleHandleInfo(StageAttemptShuffleHandleInfo.toProto(shuffleHandle))
              .build();
    } else {
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reply = RssProtos.ReassignOnStageRetryResponse.newBuilder().setStatus(code).build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getPartitionToShufflerServerWithBlockRetry(
      RssProtos.PartitionToShuffleServerRequest request,
      StreamObserver<RssProtos.ReassignOnBlockSendFailureResponse> responseObserver) {
    RssProtos.ReassignOnBlockSendFailureResponse reply;
    RssProtos.StatusCode code;
    int shuffleId = request.getShuffleId();
    MutableShuffleHandleInfo shuffleHandle =
        (MutableShuffleHandleInfo) shuffleManager.getShuffleHandleInfoByShuffleId(shuffleId);
    if (shuffleHandle != null) {
      code = RssProtos.StatusCode.SUCCESS;
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setHandle(MutableShuffleHandleInfo.toProto(shuffleHandle))
              .build();
    } else {
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reply = RssProtos.ReassignOnBlockSendFailureResponse.newBuilder().setStatus(code).build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reassignOnStageResubmit(
      RssProtos.ReassignServersRequest request,
      StreamObserver<RssProtos.ReassignServersResponse> responseObserver) {
    int stageId = request.getStageId();
    int stageAttemptNumber = request.getStageAttemptNumber();
    int shuffleId = request.getShuffleId();
    int numPartitions = request.getNumPartitions();
    boolean needReassign =
        shuffleManager.reassignOnStageResubmit(
            stageId, stageAttemptNumber, shuffleId, numPartitions);
    RssProtos.StatusCode code = RssProtos.StatusCode.SUCCESS;
    RssProtos.ReassignServersResponse reply =
        RssProtos.ReassignServersResponse.newBuilder()
            .setStatus(code)
            .setNeedReassign(needReassign)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reassignOnBlockSendFailure(
      org.apache.uniffle.proto.RssProtos.RssReassignOnBlockSendFailureRequest request,
      io.grpc.stub.StreamObserver<
              org.apache.uniffle.proto.RssProtos.ReassignOnBlockSendFailureResponse>
          responseObserver) {
    RssProtos.StatusCode code = RssProtos.StatusCode.INTERNAL_ERROR;
    RssProtos.ReassignOnBlockSendFailureResponse reply;
    try {
      LOG.info(
          "Accepted reassign request on block sent failure for shuffleId: {}, stageId: {}, stageAttemptNumber: {} from taskAttemptId: {} on executorId: {}",
          request.getShuffleId(),
          request.getStageId(),
          request.getStageAttemptNumber(),
          request.getTaskAttemptId(),
          request.getExecutorId());
      MutableShuffleHandleInfo handle =
          shuffleManager.reassignOnBlockSendFailure(
              request.getStageId(),
              request.getStageAttemptNumber(),
              request.getShuffleId(),
              request.getFailurePartitionToServerIdsMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey, x -> ReceivingFailureServer.fromProto(x.getValue()))));
      code = RssProtos.StatusCode.SUCCESS;
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setHandle(MutableShuffleHandleInfo.toProto(handle))
              .build();
    } catch (Exception e) {
      LOG.error("Errors on reassigning when block send failure.", e);
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setMsg(e.getMessage())
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * Remove the no longer used shuffle id's rss shuffle status. This is called when ShuffleManager
   * unregisters the corresponding shuffle id.
   *
   * @param shuffleId the shuffle id to unregister.
   */
  public void unregisterShuffle(int shuffleId) {
    shuffleStatus.remove(shuffleId);
  }

  private static class ShuffleServerFailureRecord {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final Map<String, AtomicInteger> shuffleServerFailureRecordCount;
    private int stageAttemptNumber;

    private ShuffleServerFailureRecord(
        Map<String, AtomicInteger> shuffleServerFailureRecordCount, int stageAttemptNumber) {
      this.shuffleServerFailureRecordCount = shuffleServerFailureRecordCount;
      this.stageAttemptNumber = stageAttemptNumber;
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

    public int getStageAttempt() {
      return withReadLock(() -> this.stageAttemptNumber);
    }

    public boolean resetStageAttemptIfNecessary(int stageAttemptNumber) {
      return withWriteLock(
          () -> {
            if (this.stageAttemptNumber < stageAttemptNumber) {
              // a new stage attempt is issued. Record the shuffleServer status of the Map should be
              // clear and reset.
              shuffleServerFailureRecordCount.clear();
              this.stageAttemptNumber = stageAttemptNumber;
              return false;
            } else if (this.stageAttemptNumber > stageAttemptNumber) {
              return true;
            }
            return false;
          });
    }

    public boolean incPartitionWriteFailure(
        int stageAttemptNumber,
        List<ShuffleServerInfo> shuffleServerInfos,
        RssShuffleManagerInterface shuffleManager) {
      return withWriteLock(
          () -> {
            if (this.stageAttemptNumber != stageAttemptNumber) {
              // do nothing here
              return false;
            }
            shuffleServerInfos.forEach(
                shuffleServerInfo -> {
                  shuffleServerFailureRecordCount
                      .computeIfAbsent(shuffleServerInfo.getId(), k -> new AtomicInteger())
                      .incrementAndGet();
                });
            List<Map.Entry<String, AtomicInteger>> list =
                new ArrayList(shuffleServerFailureRecordCount.entrySet());
            boolean retry = false;
            for (Map.Entry<String, AtomicInteger> shuffleServerInfoIntegerEntry : list) {
              if (shuffleServerInfoIntegerEntry.getValue().get()
                  > shuffleManager.getMaxFetchFailures()) {
                shuffleManager.addFailuresShuffleServerInfos(
                    shuffleServerInfoIntegerEntry.getKey());
                retry = true;
              }
            }
            return retry;
          });
    }
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
     * Check whether the input stage attempt is a new stage or not. If a new stage attempt is
     * requested, reset partitions.
     *
     * @param stageAttempt the incoming stage attempt number
     * @return 0 if stageAttempt == this.stageAttempt 1 if stageAttempt > this.stageAttempt -1 if
     *     stateAttempt < this.stageAttempt which means nothing happens
     */
    public int resetStageAttemptIfNecessary(int stageAttempt) {
      return withWriteLock(
          () -> {
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
      withWriteLock(
          () -> {
            if (this.stageAttempt != stageAttempt) {
              // do nothing here
            } else {
              this.partitions[partition] = this.partitions[partition] + 1;
            }
            return null;
          });
    }

    public int getPartitionFetchFailureNum(int stageAttempt, int partition) {
      return withReadLock(
          () -> {
            if (this.stageAttempt != stageAttempt) {
              return 0;
            } else {
              return this.partitions[partition];
            }
          });
    }
  }

  @Override
  public void getShuffleResult(
      RssProtos.GetShuffleResultRequest request,
      StreamObserver<RssProtos.GetShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.GetShuffleResultResponse reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    Roaring64NavigableMap blockIdBitmap = blockIdManager.get(shuffleId, partitionId);
    RssProtos.GetShuffleResultResponse reply;
    try {
      byte[] serializeBitmap = RssUtils.serializeBitMap(blockIdBitmap);
      reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.SUCCESS)
              .setSerializedBitmap(UnsafeByteOperations.unsafeWrap(serializeBitmap))
              .build();
    } catch (Exception exception) {
      LOG.error("Errors on getting the blockId bitmap.", exception);
      reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.INTERNAL_ERROR)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResultForMultiPart(
      RssProtos.GetShuffleResultForMultiPartRequest request,
      StreamObserver<RssProtos.GetShuffleResultForMultiPartResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.GetShuffleResultForMultiPartResponse reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    int shuffleId = request.getShuffleId();
    List<Integer> partitionIds = request.getPartitionsList();

    Roaring64NavigableMap blockIdBitmapCollection = Roaring64NavigableMap.bitmapOf();
    for (int partitionId : partitionIds) {
      Roaring64NavigableMap blockIds = blockIdManager.get(shuffleId, partitionId);
      blockIds.forEach(x -> blockIdBitmapCollection.add(x));
    }

    RssProtos.GetShuffleResultForMultiPartResponse reply;
    try {
      byte[] serializeBitmap = RssUtils.serializeBitMap(blockIdBitmapCollection);
      reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.SUCCESS)
              .setSerializedBitmap(UnsafeByteOperations.unsafeWrap(serializeBitmap))
              .build();
    } catch (Exception exception) {
      LOG.error("Errors on getting the blockId bitmap.", exception);
      reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.INTERNAL_ERROR)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleResult(
      RssProtos.ReportShuffleResultRequest request,
      StreamObserver<RssProtos.ReportShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.ReportShuffleResultResponse reply =
          RssProtos.ReportShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    int shuffleId = request.getShuffleId();

    for (RssProtos.PartitionToBlockIds partitionToBlockIds : request.getPartitionToBlockIdsList()) {
      int partitionId = partitionToBlockIds.getPartitionId();
      List<Long> blockIds = partitionToBlockIds.getBlockIdsList();
      blockIdManager.add(shuffleId, partitionId, blockIds);
    }

    RssProtos.ReportShuffleResultResponse reply =
        RssProtos.ReportShuffleResultResponse.newBuilder()
            .setStatus(RssProtos.StatusCode.SUCCESS)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
