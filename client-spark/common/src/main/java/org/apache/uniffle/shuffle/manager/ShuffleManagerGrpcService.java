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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.shuffle.RssStageResubmitManager;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.apache.spark.shuffle.stage.RssShuffleStatus;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleManagerGrpc.ShuffleManagerImplBase;
import org.apache.uniffle.shuffle.BlockIdManager;

public class ShuffleManagerGrpcService extends ShuffleManagerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcService.class);
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
    int stageId = request.getStageId();
    String executorId = request.getExecutorId();
    long taskAttemptId = request.getTaskAttemptId();
    int taskAttemptNumber = request.getTaskAttemptNumber();

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
      RssStageResubmitManager stageResubmitManager = shuffleManager.getStageResubmitManager();
      RssShuffleStatus shuffleStatus =
          stageResubmitManager.getShuffleStatusForWriter(shuffleId, stageId, stageAttemptNumber);
      if (shuffleStatus == null) {
        msg =
            String.format(
                "got an old stage(%d:%d) shuffle(%d) write failure report from executor(%s), task(%d:%d) which should be impossible.",
                stageId,
                stageAttemptNumber,
                shuffleId,
                executorId,
                taskAttemptId,
                taskAttemptNumber);
        LOG.warn(msg);
        code = RssProtos.StatusCode.INVALID_REQUEST;
        reSubmitWholeStage = false;
      } else {
        code = RssProtos.StatusCode.SUCCESS;
        shuffleStatus.incTaskFailure(taskAttemptNumber);
        if (shuffleServerIdsList != null) {
          List<ShuffleServerInfo> serverInfos = ShuffleServerInfo.fromProto(shuffleServerIdsList);
          serverInfos.stream().forEach(x -> stageResubmitManager.addBlackListedServer(x.getId()));
        }
        if (stageResubmitManager.activateStageRetry(shuffleStatus)) {
          reSubmitWholeStage = true;
          msg =
              String.format(
                  "Activate stage retry for writer on stage(%d:%d), taskFailuresCount:(%d)",
                  stageId, stageAttemptNumber, shuffleStatus.getTaskFailureAttemptCount());
          int partitionNum = shuffleManager.getPartitionNum(shuffleId);
          Object shuffleLock = stageResubmitManager.getOrCreateShuffleLock(shuffleId);
          synchronized (shuffleLock) {
            if (shuffleManager.reassignOnStageResubmit(
                stageId, stageAttemptNumber, shuffleId, partitionNum)) {
              LOG.info(
                  "{} from executorId({}), task({}:{}) on stageId({}:{}), shuffleId({})",
                  msg,
                  executorId,
                  taskAttemptId,
                  taskAttemptNumber,
                  stageId,
                  stageAttemptNumber,
                  shuffleId);
            }
            shuffleStatus.markStageAttemptRetried();
          }
        } else {
          reSubmitWholeStage = false;
          msg = "Accepted task write failure report";
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
    int shuffleId = request.getShuffleId();
    int stageId = request.getStageId();
    long taskAttemptId = request.getTaskAttemptId();
    int taskAttemptNumber = request.getTaskAttemptNumber();
    String executorId = request.getExecutorId();
    List<RssProtos.ShuffleServerId> serverIds = request.getFetchFailureServerIdList();

    RssStageResubmitManager stageResubmitManager = shuffleManager.getStageResubmitManager();
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
      RssShuffleStatus rssShuffleStatus =
          stageResubmitManager.getShuffleStatusForReader(shuffleId, stageId, stageAttempt);
      if (rssShuffleStatus == null) {
        msg =
            String.format(
                "got an old stage(%d:%d) shuffle(%d) fetch failure report from executor(%s), task(%d:%d) which should be impossible.",
                stageId, stageAttempt, shuffleId, executorId, taskAttemptId, taskAttemptNumber);
        LOG.warn(msg);
        code = RssProtos.StatusCode.INVALID_REQUEST;
        reSubmitWholeStage = false;
      } else {
        code = RssProtos.StatusCode.SUCCESS;
        rssShuffleStatus.incTaskFailure(taskAttemptNumber);
        if (CollectionUtils.isNotEmpty(serverIds)) {
          ShuffleServerInfo.fromProto(serverIds).stream()
              .map(x -> x.getId())
              .forEach(x -> stageResubmitManager.addBlackListedServer(x));
        }
        if (stageResubmitManager.activateStageRetry(rssShuffleStatus)) {
          reSubmitWholeStage = true;
          msg =
              String.format(
                  "Activate stage retry for reader on stage(%d:%d), taskFailuresCount:(%d)",
                  stageId, stageAttempt, rssShuffleStatus.getTaskFailureAttemptCount());
          int partitionNum = shuffleManager.getPartitionNum(shuffleId);
          Object shuffleLock = stageResubmitManager.getOrCreateShuffleLock(shuffleId);
          synchronized (shuffleLock) {
            if (shuffleManager.reassignOnStageResubmit(
                stageId, stageAttempt, shuffleId, partitionNum)) {
              LOG.info(
                  "{} from executorId({}), task({}:{}) on stageId({}:{}), shuffleId({})",
                  msg,
                  executorId,
                  taskAttemptId,
                  taskAttemptNumber,
                  stageId,
                  stageAttempt,
                  shuffleId);
            }
            rssShuffleStatus.markStageAttemptRetried();
          }
        } else {
          reSubmitWholeStage = false;
          msg = "Accepted task fetch failure report";
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
    shuffleManager.getStageResubmitManager().clear(shuffleId);
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
