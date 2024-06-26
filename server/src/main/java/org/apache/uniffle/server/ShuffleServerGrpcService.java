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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.exception.NoBufferForHugePartitionException;
import org.apache.uniffle.common.exception.NoRegisterException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatRequest;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatResponse;
import org.apache.uniffle.proto.RssProtos.FinishShuffleRequest;
import org.apache.uniffle.proto.RssProtos.FinishShuffleResponse;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexResponse;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultForMultiPartRequest;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultForMultiPartResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultResponse;
import org.apache.uniffle.proto.RssProtos.PartitionToBlockIds;
import org.apache.uniffle.proto.RssProtos.RemoteStorageConfItem;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultResponse;
import org.apache.uniffle.proto.RssProtos.RequireBufferRequest;
import org.apache.uniffle.proto.RssProtos.RequireBufferResponse;
import org.apache.uniffle.proto.RssProtos.SendShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.SendShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleBlock;
import org.apache.uniffle.proto.RssProtos.ShuffleCommitRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleCommitResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleData;
import org.apache.uniffle.proto.RssProtos.ShuffleDataBlockSegment;
import org.apache.uniffle.proto.RssProtos.ShufflePartitionRange;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterResponse;
import org.apache.uniffle.proto.ShuffleServerGrpc.ShuffleServerImplBase;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class ShuffleServerGrpcService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcService.class);
  private final ShuffleServer shuffleServer;

  public ShuffleServerGrpcService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  @Override
  public void unregisterShuffleByAppId(
      RssProtos.ShuffleUnregisterByAppIdRequest request,
      StreamObserver<RssProtos.ShuffleUnregisterByAppIdResponse> responseStreamObserver) {
    String appId = request.getAppId();

    StatusCode result = StatusCode.SUCCESS;
    String responseMessage = "OK";
    try {
      shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId);

    } catch (Exception e) {
      result = StatusCode.INTERNAL_ERROR;
    }

    RssProtos.ShuffleUnregisterByAppIdResponse reply =
        RssProtos.ShuffleUnregisterByAppIdResponse.newBuilder()
            .setStatus(result.toProto())
            .setRetMsg(responseMessage)
            .build();
    responseStreamObserver.onNext(reply);
    responseStreamObserver.onCompleted();
  }

  @Override
  public void unregisterShuffle(
      RssProtos.ShuffleUnregisterRequest request,
      StreamObserver<RssProtos.ShuffleUnregisterResponse> responseStreamObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();

    StatusCode result = StatusCode.SUCCESS;
    String responseMessage = "OK";
    try {
      shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId, shuffleId);
    } catch (Exception e) {
      result = StatusCode.INTERNAL_ERROR;
    }

    RssProtos.ShuffleUnregisterResponse reply =
        RssProtos.ShuffleUnregisterResponse.newBuilder()
            .setStatus(result.toProto())
            .setRetMsg(responseMessage)
            .build();
    responseStreamObserver.onNext(reply);
    responseStreamObserver.onCompleted();
  }

  @Override
  public void registerShuffle(
      ShuffleRegisterRequest req, StreamObserver<ShuffleRegisterResponse> responseObserver) {

    ShuffleRegisterResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    String remoteStoragePath = req.getRemoteStorage().getPath();
    String user = req.getUser();
    int stageAttemptNumber = req.getStageAttemptNumber();
    // If the Stage is registered for the first time, you do not need to consider the Stage retry
    // and delete the Block data that has been sent.
    if (stageAttemptNumber > 0) {
      ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
      // Prevents AttemptNumber of multiple stages from modifying the latest AttemptNumber.
      synchronized (taskInfo) {
        int attemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
        if (stageAttemptNumber > attemptNumber) {
          taskInfo.refreshLatestStageAttemptNumber(shuffleId, stageAttemptNumber);
          try {
            long start = System.currentTimeMillis();
            shuffleServer.getShuffleTaskManager().removeShuffleDataSync(appId, shuffleId);
            LOG.info(
                "Deleted the previous stage attempt data due to stage recomputing for app: {}, "
                    + "shuffleId: {}. It costs {} ms",
                appId,
                shuffleId,
                System.currentTimeMillis() - start);
          } catch (Exception e) {
            LOG.error(
                "Errors on clearing previous stage attempt data for app: {}, shuffleId: {}",
                appId,
                shuffleId,
                e);
            StatusCode code = StatusCode.INTERNAL_ERROR;
            reply = ShuffleRegisterResponse.newBuilder().setStatus(code.toProto()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
          }
        } else if (stageAttemptNumber < attemptNumber) {
          // When a Stage retry occurs, the first or last registration of a Stage may need to be
          // ignored and the ignored status quickly returned.
          StatusCode code = StatusCode.STAGE_RETRY_IGNORE;
          reply = ShuffleRegisterResponse.newBuilder().setStatus(code.toProto()).build();
          responseObserver.onNext(reply);
          responseObserver.onCompleted();
          return;
        }
      }
    }

    ShuffleDataDistributionType shuffleDataDistributionType =
        ShuffleDataDistributionType.valueOf(
            Optional.ofNullable(req.getShuffleDataDistribution())
                .orElse(RssProtos.DataDistribution.NORMAL)
                .name());

    int maxConcurrencyPerPartitionToWrite = req.getMaxConcurrencyPerPartitionToWrite();

    Map<String, String> remoteStorageConf =
        req.getRemoteStorage().getRemoteStorageConfList().stream()
            .collect(
                Collectors.toMap(RemoteStorageConfItem::getKey, RemoteStorageConfItem::getValue));

    List<PartitionRange> partitionRanges = toPartitionRanges(req.getPartitionRangesList());
    LOG.info(
        "Get register request for appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], remoteStorage["
            + remoteStoragePath
            + "] with "
            + partitionRanges.size()
            + " partition ranges. User: {}",
        user);

    StatusCode result =
        shuffleServer
            .getShuffleTaskManager()
            .registerShuffle(
                appId,
                shuffleId,
                partitionRanges,
                new RemoteStorageInfo(remoteStoragePath, remoteStorageConf),
                user,
                shuffleDataDistributionType,
                maxConcurrencyPerPartitionToWrite);

    reply = ShuffleRegisterResponse.newBuilder().setStatus(result.toProto()).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void sendShuffleData(
      SendShuffleDataRequest req, StreamObserver<SendShuffleDataResponse> responseObserver) {

    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    long requireBufferId = req.getRequireBufferId();
    long timestamp = req.getTimestamp();
    int stageAttemptNumber = req.getStageAttemptNumber();
    ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
    Integer latestStageAttemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
    // The Stage retry occurred, and the task before StageNumber was simply ignored and not
    // processed if the task was being sent.
    if (stageAttemptNumber < latestStageAttemptNumber) {
      String responseMessage = "A retry has occurred at the Stage, sending data is invalid.";
      reply =
          SendShuffleDataResponse.newBuilder()
              .setStatus(StatusCode.STAGE_RETRY_IGNORE.toProto())
              .setRetMsg(responseMessage)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }
    if (timestamp > 0) {
      /*
       * Here we record the transport time, but we don't consider the impact of data size on transport time.
       * The amount of data will not cause great fluctuations in latency. For example, 100K costs 1ms,
       * and 1M costs 10ms. This seems like a normal fluctuation, but it may rise to 10s when the server load is high.
       * In addition, we need to pay attention to that the time of the client machine and the machine
       * time of the Shuffle Server should be kept in sync. TransportTime is accurate only if this condition is met.
       * */
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getGrpcMetrics()
            .recordTransportTime(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD, transportTime);
      }
    }
    int requireSize = shuffleServer.getShuffleTaskManager().getRequireBufferSize(requireBufferId);

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getShuffleDataCount() > 0) {
      ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireSize);
      ShuffleTaskManager manager = shuffleServer.getShuffleTaskManager();
      PreAllocatedBufferInfo info = manager.getAndRemovePreAllocatedBuffer(requireBufferId);
      boolean isPreAllocated = info != null;
      if (!isPreAllocated) {
        String errorMsg =
            "Can't find requireBufferId["
                + requireBufferId
                + "] for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]";
        LOG.warn(errorMsg);
        responseMessage = errorMsg;
        reply =
            SendShuffleDataResponse.newBuilder()
                .setStatus(StatusCode.INTERNAL_ERROR.toProto())
                .setRetMsg(responseMessage)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
      final long start = System.currentTimeMillis();
      List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(req);
      long alreadyReleasedSize = 0;
      boolean hasFailureOccurred = false;
      for (ShufflePartitionedData spd : shufflePartitionedData) {
        String shuffleDataInfo =
            "appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], partitionId["
                + spd.getPartitionId()
                + "]";
        try {
          ret = manager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
          if (ret != StatusCode.SUCCESS) {
            String errorMsg =
                "Error happened when shuffleEngine.write for "
                    + shuffleDataInfo
                    + ", statusCode="
                    + ret;
            LOG.error(errorMsg);
            responseMessage = errorMsg;
            hasFailureOccurred = true;
            break;
          } else {
            long toReleasedSize = spd.getTotalBlockSize();
            // after each cacheShuffleData call, the `preAllocatedSize` is updated timely.
            manager.releasePreAllocatedSize(toReleasedSize);
            alreadyReleasedSize += toReleasedSize;
            manager.updateCachedBlockIds(
                appId, shuffleId, spd.getPartitionId(), spd.getBlockList());
          }
        } catch (Exception e) {
          String errorMsg =
              "Error happened when shuffleEngine.write for "
                  + shuffleDataInfo
                  + ": "
                  + e.getMessage();
          ret = StatusCode.INTERNAL_ERROR;
          responseMessage = errorMsg;
          LOG.error(errorMsg);
          hasFailureOccurred = true;
          break;
        } finally {
          if (hasFailureOccurred) {
            shuffleServer
                .getShuffleBufferManager()
                .releaseMemory(spd.getTotalBlockSize(), false, false);
          }
        }
      }
      // since the required buffer id is only used once, the shuffle client would try to require
      // another buffer whether
      // current connection succeeded or not. Therefore, the preAllocatedBuffer is first get and
      // removed, then after
      // cacheShuffleData finishes, the preAllocatedSize should be updated accordingly.
      if (info.getRequireSize() > alreadyReleasedSize) {
        manager.releasePreAllocatedSize(info.getRequireSize() - alreadyReleasedSize);
      }
      reply =
          SendShuffleDataResponse.newBuilder()
              .setStatus(ret.toProto())
              .setRetMsg(responseMessage)
              .build();
      long costTime = System.currentTimeMillis() - start;
      shuffleServer
          .getGrpcMetrics()
          .recordProcessTime(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD, costTime);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cache Shuffle Data for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], cost "
                + costTime
                + " ms with "
                + shufflePartitionedData.size()
                + " blocks and "
                + requireSize
                + " bytes");
      }
    } else {
      reply =
          SendShuffleDataResponse.newBuilder()
              .setStatus(StatusCode.INTERNAL_ERROR.toProto())
              .setRetMsg("No data in request")
              .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void commitShuffleTask(
      ShuffleCommitRequest req, StreamObserver<ShuffleCommitResponse> responseObserver) {

    ShuffleCommitResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();

    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    int commitCount = 0;

    try {
      if (!shuffleServer.getShuffleTaskManager().getAppIds().contains(appId)) {
        throw new IllegalStateException("AppId " + appId + " was removed already");
      }
      commitCount = shuffleServer.getShuffleTaskManager().updateAndGetCommitCount(appId, shuffleId);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Get commitShuffleTask request for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], currentCommitted["
                + commitCount
                + "]");
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = "Error happened when commit for appId[" + appId + "], shuffleId[" + shuffleId + "]";
      LOG.error(msg, e);
    }

    reply =
        ShuffleCommitResponse.newBuilder()
            .setCommitCount(commitCount)
            .setStatus(status.toProto())
            .setRetMsg(msg)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void finishShuffle(
      FinishShuffleRequest req, StreamObserver<FinishShuffleResponse> responseObserver) {
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    StatusCode status;
    String msg = "OK";
    String errorMsg =
        "Fail to finish shuffle for appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], data may be lost";
    try {
      LOG.info("Get finishShuffle request for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      status = shuffleServer.getShuffleTaskManager().commitShuffle(appId, shuffleId);
      if (status != StatusCode.SUCCESS) {
        status = StatusCode.INTERNAL_ERROR;
        msg = errorMsg;
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = errorMsg;
      LOG.error(errorMsg, e);
    }

    FinishShuffleResponse response =
        FinishShuffleResponse.newBuilder().setStatus(status.toProto()).setRetMsg(msg).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void requireBuffer(
      RequireBufferRequest request, StreamObserver<RequireBufferResponse> responseObserver) {
    String appId = request.getAppId();
    long requireBufferId = -1;
    StatusCode status = StatusCode.SUCCESS;
    try {
      if (StringUtils.isEmpty(appId)) {
        // To be compatible with older client version
        requireBufferId =
            shuffleServer.getShuffleTaskManager().requireBuffer(request.getRequireSize());
      } else {
        requireBufferId =
            shuffleServer
                .getShuffleTaskManager()
                .requireBuffer(
                    appId,
                    request.getShuffleId(),
                    request.getPartitionIdsList(),
                    request.getRequireSize());
      }
    } catch (NoBufferException e) {
      status = StatusCode.NO_BUFFER;
      ShuffleServerMetrics.counterTotalRequireBufferFailedForRegularPartition.inc();
      ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
    } catch (NoBufferForHugePartitionException e) {
      status = StatusCode.NO_BUFFER_FOR_HUGE_PARTITION;
      ShuffleServerMetrics.counterTotalRequireBufferFailedForHugePartition.inc();
      ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
    } catch (NoRegisterException e) {
      status = StatusCode.NO_REGISTER;
      ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
    }
    RequireBufferResponse response =
        RequireBufferResponse.newBuilder()
            .setStatus(status.toProto())
            .setRequireBufferId(requireBufferId)
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void appHeartbeat(
      AppHeartBeatRequest request, StreamObserver<AppHeartBeatResponse> responseObserver) {
    String appId = request.getAppId();
    shuffleServer.getShuffleTaskManager().refreshAppId(appId);
    AppHeartBeatResponse response =
        AppHeartBeatResponse.newBuilder()
            .setRetMsg("")
            .setStatus(StatusCode.SUCCESS.toProto())
            .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Cancelled by client {} for after deadline.", appId);
      return;
    }

    LOG.info("Get heartbeat from {}", appId);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleResult(
      ReportShuffleResultRequest request,
      StreamObserver<ReportShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    long taskAttemptId = request.getTaskAttemptId();
    int bitmapNum = request.getBitmapNum();
    Map<Integer, long[]> partitionToBlockIds =
        toPartitionBlocksMap(request.getPartitionToBlockIdsList());
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    ReportShuffleResultResponse reply;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], taskAttemptId[" + taskAttemptId + "]";

    try {
      int expectedBlockCount = partitionToBlockIds.values().stream().mapToInt(x -> x.length).sum();
      LOG.info(
          "Accepted blockIds report for {} blocks across {} partitions as shuffle result for task {}",
          expectedBlockCount,
          partitionToBlockIds.size(),
          requestInfo);
      int updatedBlockCount =
          shuffleServer
              .getShuffleTaskManager()
              .addFinishedBlockIds(appId, shuffleId, partitionToBlockIds, bitmapNum);
      if (expectedBlockCount != updatedBlockCount) {
        LOG.warn(
            "Existing {} duplicated blockIds on blockId report for appId: {}, shuffleId: {}",
            expectedBlockCount - updatedBlockCount,
            appId,
            shuffleId);
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = "error happened when report shuffle result, check shuffle server for detail";
      LOG.error("Error happened when report shuffle result for " + requestInfo, e);
    }

    reply =
        ReportShuffleResultResponse.newBuilder().setStatus(status.toProto()).setRetMsg(msg).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResult(
      GetShuffleResultRequest request, StreamObserver<GetShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    BlockIdLayout blockIdLayout =
        BlockIdLayout.from(
            request.getBlockIdLayout().getSequenceNoBits(),
            request.getBlockIdLayout().getPartitionIdBits(),
            request.getBlockIdLayout().getTaskAttemptIdBits());
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleResultResponse reply;
    byte[] serializedBlockIds = null;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";
    ByteString serializedBlockIdsBytes = ByteString.EMPTY;

    try {
      serializedBlockIds =
          shuffleServer
              .getShuffleTaskManager()
              .getFinishedBlockIds(appId, shuffleId, Sets.newHashSet(partitionId), blockIdLayout);
      if (serializedBlockIds == null) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Can't get shuffle result for " + requestInfo;
        LOG.warn(msg);
      } else {
        serializedBlockIdsBytes = UnsafeByteOperations.unsafeWrap(serializedBlockIds);
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
      LOG.error("Error happened when get shuffle result for {}", requestInfo, e);
    }

    reply =
        GetShuffleResultResponse.newBuilder()
            .setStatus(status.toProto())
            .setRetMsg(msg)
            .setSerializedBitmap(serializedBlockIdsBytes)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResultForMultiPart(
      GetShuffleResultForMultiPartRequest request,
      StreamObserver<GetShuffleResultForMultiPartResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    List<Integer> partitionsList = request.getPartitionsList();
    BlockIdLayout blockIdLayout =
        BlockIdLayout.from(
            request.getBlockIdLayout().getSequenceNoBits(),
            request.getBlockIdLayout().getPartitionIdBits(),
            request.getBlockIdLayout().getTaskAttemptIdBits());

    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleResultForMultiPartResponse reply;

    try {
      ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
      if (taskInfo != null) {
        synchronized (taskInfo) {
          int latestAttemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
          if (request.getStageAttemptNumber() != latestAttemptNumber) {
            LOG.error("Abort this request with the old stageAttemptNumber:{}. latest: {}", request.getStageAttemptNumber(), latestAttemptNumber);
            reply =
                GetShuffleResultForMultiPartResponse.newBuilder()
                    .setStatus(StatusCode.INTERNAL_ERROR.toProto())
                    .setRetMsg("Stage retry. Abort this request.")
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
          }
        }
      } else {
        LOG.warn("TaskInfo is null. This should not happen");
      }
    } catch (Exception e) {
      LOG.info("Errors on getting shuffle result with multi-parts.", e);
    }

    byte[] serializedBlockIds = null;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitions" + partitionsList;
    ByteString serializedBlockIdsBytes = ByteString.EMPTY;

    try {
      serializedBlockIds =
          shuffleServer
              .getShuffleTaskManager()
              .getFinishedBlockIds(
                  appId, shuffleId, Sets.newHashSet(partitionsList), blockIdLayout);
      if (serializedBlockIds == null) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Can't get shuffle result for " + requestInfo;
        LOG.warn(msg);
      } else {
        serializedBlockIdsBytes = UnsafeByteOperations.unsafeWrap(serializedBlockIds);
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
      LOG.error("Error happened when get shuffle result for {}", requestInfo, e);
    }

    reply =
        GetShuffleResultForMultiPartResponse.newBuilder()
            .setStatus(status.toProto())
            .setRetMsg(msg)
            .setSerializedBitmap(serializedBlockIdsBytes)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getLocalShuffleData(
      GetLocalShuffleDataRequest request,
      StreamObserver<GetLocalShuffleDataResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    int partitionNumPerRange = request.getPartitionNumPerRange();
    int partitionNum = request.getPartitionNum();
    long offset = request.getOffset();
    int length = request.getLength();
    long timestamp = request.getTimestamp();
    if (timestamp > 0) {
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getGrpcMetrics()
            .recordTransportTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD, transportTime);
      }
    }
    String storageType =
        shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE).name();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetLocalShuffleDataResponse reply = null;
    ShuffleDataResult sdr = null;
    String requestInfo =
        "appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], partitionId["
            + partitionId
            + "]"
            + "offset["
            + offset
            + "]"
            + "length["
            + length
            + "]";

    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        shuffleServer
            .getStorageManager()
            .selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
    if (storage != null) {
      storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
    }

    if (shuffleServer.getShuffleBufferManager().requireReadMemory(length)) {
      try {
        long start = System.currentTimeMillis();
        sdr =
            shuffleServer
                .getShuffleTaskManager()
                .getShuffleData(
                    appId,
                    shuffleId,
                    partitionId,
                    partitionNumPerRange,
                    partitionNum,
                    storageType,
                    offset,
                    length);
        long readTime = System.currentTimeMillis() - start;
        ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
        ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getDataLength());
        ShuffleServerMetrics.counterTotalReadLocalDataFileSize.inc(sdr.getDataLength());
        ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.inc();
        ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.inc(length);
        shuffleServer
            .getGrpcMetrics()
            .recordProcessTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD, readTime);
        LOG.info(
            "Successfully getShuffleData cost {} ms for shuffle data with {}",
            readTime,
            requestInfo);
        reply =
            GetLocalShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .setData(UnsafeByteOperations.unsafeWrap(sdr.getData()))
                .build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        reply =
            GetLocalShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      } finally {
        if (sdr != null) {
          sdr.release();
          ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.dec();
          ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.dec(length);
        }
        shuffleServer.getShuffleBufferManager().releaseReadMemory(length);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get shuffle data";
      LOG.warn("{} for {}", msg, requestInfo);
      reply =
          GetLocalShuffleDataResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getLocalShuffleIndex(
      GetLocalShuffleIndexRequest request,
      StreamObserver<GetLocalShuffleIndexResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    int partitionNumPerRange = request.getPartitionNumPerRange();
    int partitionNum = request.getPartitionNum();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetLocalShuffleIndexResponse reply;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        shuffleServer
            .getStorageManager()
            .selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
    if (storage != null) {
      storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
    }
    // Index file is expected small size and won't cause oom problem with the assumed size. An index
    // segment is 40B,
    // with the default size - 2MB, it can support 50k blocks for shuffle data.
    long assumedFileSize =
        shuffleServer
            .getShuffleServerConf()
            .getLong(ShuffleServerConf.SERVER_SHUFFLE_INDEX_SIZE_HINT);
    if (shuffleServer.getShuffleBufferManager().requireReadMemory(assumedFileSize)) {
      ShuffleIndexResult shuffleIndexResult = null;
      try {
        final long start = System.currentTimeMillis();
        shuffleIndexResult =
            shuffleServer
                .getShuffleTaskManager()
                .getShuffleIndex(appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);

        ByteBuffer data = shuffleIndexResult.getIndexData();
        ShuffleServerMetrics.counterTotalReadDataSize.inc(data.remaining());
        ShuffleServerMetrics.counterTotalReadLocalIndexFileSize.inc(data.remaining());
        ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.inc();
        ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.inc(assumedFileSize);
        GetLocalShuffleIndexResponse.Builder builder =
            GetLocalShuffleIndexResponse.newBuilder().setStatus(status.toProto()).setRetMsg(msg);
        long readTime = System.currentTimeMillis() - start;
        shuffleServer
            .getGrpcMetrics()
            .recordProcessTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_INDEX_METHOD, readTime);
        LOG.info(
            "Successfully getShuffleIndex cost {} ms for {} bytes with {}",
            readTime,
            data.remaining(),
            requestInfo);

        builder.setIndexData(UnsafeByteOperations.unsafeWrap(data));
        builder.setDataFileLen(shuffleIndexResult.getDataFileLen());
        reply = builder.build();
      } catch (FileNotFoundException indexFileNotFoundException) {
        LOG.warn(
            "Index file for {} is not found, maybe the data has been flushed to cold storage.",
            requestInfo,
            indexFileNotFoundException);
        reply = GetLocalShuffleIndexResponse.newBuilder().setStatus(status.toProto()).build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle index for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        reply =
            GetLocalShuffleIndexResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      } finally {
        if (shuffleIndexResult != null) {
          shuffleIndexResult.release();
          ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.dec();
          ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.dec(assumedFileSize);
        }
        shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get shuffle index";
      LOG.warn("{} for {}", msg, requestInfo);
      reply =
          GetLocalShuffleIndexResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getMemoryShuffleData(
      GetMemoryShuffleDataRequest request,
      StreamObserver<GetMemoryShuffleDataResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    long blockId = request.getLastBlockId();
    int readBufferSize = request.getReadBufferSize();
    long timestamp = request.getTimestamp();

    if (timestamp > 0) {
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getGrpcMetrics()
            .recordTransportTime(
                ShuffleServerGrpcMetrics.GET_MEMORY_SHUFFLE_DATA_METHOD, transportTime);
      }
    }
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetMemoryShuffleDataResponse reply;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    // todo: if can get the exact memory size?
    if (shuffleServer.getShuffleBufferManager().requireReadMemory(readBufferSize)) {
      ShuffleDataResult shuffleDataResult = null;
      try {
        final long start = System.currentTimeMillis();
        Roaring64NavigableMap expectedTaskIds = null;
        if (request.getSerializedExpectedTaskIdsBitmap() != null
            && !request.getSerializedExpectedTaskIdsBitmap().isEmpty()) {
          expectedTaskIds =
              RssUtils.deserializeBitMap(
                  request.getSerializedExpectedTaskIdsBitmap().toByteArray());
        }
        shuffleDataResult =
            shuffleServer
                .getShuffleTaskManager()
                .getInMemoryShuffleData(
                    appId, shuffleId, partitionId, blockId, readBufferSize, expectedTaskIds);
        byte[] data = new byte[] {};
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        if (shuffleDataResult != null) {
          data = shuffleDataResult.getData();
          bufferSegments = shuffleDataResult.getBufferSegments();
          ShuffleServerMetrics.counterTotalReadDataSize.inc(data.length);
          ShuffleServerMetrics.counterTotalReadMemoryDataSize.inc(data.length);
          ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.inc();
          ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.inc(readBufferSize);
        }
        long costTime = System.currentTimeMillis() - start;
        shuffleServer
            .getGrpcMetrics()
            .recordProcessTime(ShuffleServerGrpcMetrics.GET_MEMORY_SHUFFLE_DATA_METHOD, costTime);
        LOG.info(
            "Successfully getInMemoryShuffleData cost {} ms with {} bytes shuffle data for {}",
            costTime,
            data.length,
            requestInfo);

        reply =
            GetMemoryShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .setData(UnsafeByteOperations.unsafeWrap(data))
                .addAllShuffleDataBlockSegments(toShuffleDataBlockSegments(bufferSegments))
                .build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg =
            "Error happened when get in memory shuffle data for "
                + requestInfo
                + ", "
                + e.getMessage();
        LOG.error(msg, e);
        reply =
            GetMemoryShuffleDataResponse.newBuilder()
                .setData(UnsafeByteOperations.unsafeWrap(new byte[] {}))
                .addAllShuffleDataBlockSegments(Lists.newArrayList())
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      } finally {
        if (shuffleDataResult != null) {
          shuffleDataResult.release();
          ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.dec();
          ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.dec(readBufferSize);
        }
        shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get in memory shuffle data";
      LOG.warn("{} for {}", msg, requestInfo);
      reply =
          GetMemoryShuffleDataResponse.newBuilder()
              .setData(UnsafeByteOperations.unsafeWrap(new byte[] {}))
              .addAllShuffleDataBlockSegments(Lists.newArrayList())
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  private List<ShufflePartitionedData> toPartitionedData(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (ShuffleData data : req.getShuffleDataList()) {
      ret.add(
          new ShufflePartitionedData(
              data.getPartitionId(), toPartitionedBlock(data.getBlockList())));
    }

    return ret;
  }

  private ShufflePartitionedBlock[] toPartitionedBlock(List<ShuffleBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return new ShufflePartitionedBlock[] {};
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    int i = 0;
    for (ShuffleBlock block : blocks) {
      ByteBuf data = ByteBufUtils.byteStringToByteBuf(block.getData());
      ret[i] =
          new ShufflePartitionedBlock(
              block.getLength(),
              block.getUncompressLength(),
              block.getCrc(),
              block.getBlockId(),
              block.getTaskAttemptId(),
              data);
      i++;
    }
    return ret;
  }

  private Map<Integer, long[]> toPartitionBlocksMap(List<PartitionToBlockIds> partitionToBlockIds) {
    Map<Integer, long[]> result = Maps.newHashMap();
    for (PartitionToBlockIds ptb : partitionToBlockIds) {
      List<Long> blockIds = ptb.getBlockIdsList();
      if (blockIds != null && !blockIds.isEmpty()) {
        long[] array = new long[blockIds.size()];
        for (int i = 0; i < array.length; i++) {
          array[i] = blockIds.get(i);
        }
        result.put(ptb.getPartitionId(), array);
      }
    }
    return result;
  }

  private List<PartitionRange> toPartitionRanges(
      List<ShufflePartitionRange> shufflePartitionRanges) {
    List<PartitionRange> partitionRanges = Lists.newArrayList();
    for (ShufflePartitionRange spr : shufflePartitionRanges) {
      partitionRanges.add(new PartitionRange(spr.getStart(), spr.getEnd()));
    }
    return partitionRanges;
  }

  private List<ShuffleDataBlockSegment> toShuffleDataBlockSegments(
      List<BufferSegment> bufferSegments) {
    List<ShuffleDataBlockSegment> shuffleDataBlockSegments = Lists.newArrayList();
    if (bufferSegments != null) {
      for (BufferSegment bs : bufferSegments) {
        shuffleDataBlockSegments.add(
            ShuffleDataBlockSegment.newBuilder()
                .setBlockId(bs.getBlockId())
                .setCrc(bs.getCrc())
                .setOffset(bs.getOffset())
                .setLength(bs.getLength())
                .setTaskAttemptId(bs.getTaskAttemptId())
                .setUncompressLength(bs.getUncompressLength())
                .build());
      }
    }
    return shuffleDataBlockSegments;
  }
}
