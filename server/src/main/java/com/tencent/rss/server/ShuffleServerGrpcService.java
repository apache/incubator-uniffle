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

package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.AppHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.AppHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.FinishShuffleRequest;
import com.tencent.rss.proto.RssProtos.FinishShuffleResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.PartitionToBlockIds;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.RequireBufferRequest;
import com.tencent.rss.proto.RssProtos.RequireBufferResponse;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShuffleDataBlockSegment;
import com.tencent.rss.proto.RssProtos.ShufflePartitionRange;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerImplBase;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcService.class);
  private final ShuffleServer shuffleServer;

  public ShuffleServerGrpcService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  public static RssProtos.StatusCode valueOf(StatusCode code) {
    switch (code) {
      case SUCCESS:
        return RssProtos.StatusCode.SUCCESS;
      case DOUBLE_REGISTER:
        return RssProtos.StatusCode.DOUBLE_REGISTER;
      case NO_BUFFER:
        return RssProtos.StatusCode.NO_BUFFER;
      case INVALID_STORAGE:
        return RssProtos.StatusCode.INVALID_STORAGE;
      case NO_REGISTER:
        return RssProtos.StatusCode.NO_REGISTER;
      case NO_PARTITION:
        return RssProtos.StatusCode.NO_PARTITION;
      case TIMEOUT:
        return RssProtos.StatusCode.TIMEOUT;
      default:
        return RssProtos.StatusCode.INTERNAL_ERROR;
    }
  }

  @Override
  public void registerShuffle(ShuffleRegisterRequest req,
      StreamObserver<ShuffleRegisterResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterRegisterRequest.inc();

    ShuffleRegisterResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    List<PartitionRange> partitionRanges = toPartitionRanges(req.getPartitionRangesList());
    LOG.info("Get register request for appId[" + appId + "], shuffleId[" + shuffleId + "] with "
        + partitionRanges.size() + " partition ranges");

    StatusCode result = shuffleServer
        .getShuffleTaskManager()
        .registerShuffle(appId, shuffleId, partitionRanges);

    reply = ShuffleRegisterResponse
        .newBuilder()
        .setStatus(valueOf(result))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void sendShuffleData(SendShuffleDataRequest req,
      StreamObserver<SendShuffleDataResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterSendDataRequest.inc();

    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    long requireBufferId = req.getRequireBufferId();
    int requireSize = shuffleServer
        .getShuffleTaskManager().getRequireBufferSize(requireBufferId);

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getShuffleDataCount() > 0) {
      ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireSize);
      boolean isPreAllocated = shuffleServer.getShuffleTaskManager().isPreAllocated(requireBufferId);
      if (!isPreAllocated) {
        LOG.warn("Can't find requireBufferId[" + requireBufferId + "] for appId[" + appId
            + "], shuffleId[" + shuffleId + "]");
      }
      final long start = System.currentTimeMillis();
      List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(req);
      for (ShufflePartitionedData spd : shufflePartitionedData) {
        String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + shuffleId
            + "], partitionId[" + spd.getPartitionId() + "]";
        try {
          ret = shuffleServer
              .getShuffleTaskManager()
              .cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
          if (ret != StatusCode.SUCCESS) {
            String errorMsg = "Error happened when shuffleEngine.write for "
                + shuffleDataInfo + ", statusCode=" + ret;
            LOG.error(errorMsg);
            responseMessage = errorMsg;
            break;
          } else {
            // remove require bufferId, the memory should be updated already
            shuffleServer
                .getShuffleTaskManager().removeRequireBufferId(requireBufferId);
            shuffleServer.getShuffleTaskManager().updateCachedBlockIds(
                appId, shuffleId, spd.getBlockList());
          }
        } catch (Exception e) {
          String errorMsg = "Error happened when shuffleEngine.write for "
              + shuffleDataInfo + ": " + e.getMessage();
          ret = StatusCode.INTERNAL_ERROR;
          responseMessage = errorMsg;
          LOG.error(errorMsg);
          break;
        }
      }
      reply = SendShuffleDataResponse.newBuilder().setStatus(valueOf(ret)).setRetMsg(responseMessage).build();
      LOG.debug("Cache Shuffle Data for appId[" + appId + "], shuffleId[" + shuffleId
          + "], cost " + (System.currentTimeMillis() - start)
          + " ms with " + shufflePartitionedData.size() + " blocks and " + requireSize + " bytes");
    } else {
      reply = SendShuffleDataResponse
          .newBuilder()
          .setStatus(valueOf(StatusCode.INTERNAL_ERROR))
          .setRetMsg("No data in request")
          .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void commitShuffleTask(ShuffleCommitRequest req,
      StreamObserver<ShuffleCommitResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterCommitRequest.inc();

    ShuffleCommitResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();

    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    int commitCount = 0;

    try {
      if (!shuffleServer.getShuffleTaskManager().getAppIds().containsKey(appId)) {
        throw new IllegalStateException("AppId " + appId + " was removed already");
      }
      commitCount = shuffleServer.getShuffleTaskManager().updateAndGetCommitCount(appId, shuffleId);
      LOG.debug("Get commitShuffleTask request for appId[" + appId + "], shuffleId["
          + shuffleId + "], currentCommitted[" + commitCount + "]");
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = "Error happened when commit for appId[" + appId + "], shuffleId[" + shuffleId + "]";
      LOG.error(msg, e);
    }

    reply = ShuffleCommitResponse
        .newBuilder()
        .setCommitCount(commitCount)
        .setStatus(valueOf(status))
        .setRetMsg(msg)
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void finishShuffle(FinishShuffleRequest req,
      StreamObserver<FinishShuffleResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    StatusCode status;
    String msg = "OK";
    String errorMsg = "Fail to finish shuffle for appId["
        + appId + "], shuffleId[" + shuffleId + "], data may be lost";
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
        FinishShuffleResponse
            .newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void requireBuffer(RequireBufferRequest request,
      StreamObserver<RequireBufferResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    long requireBufferId = shuffleServer.getShuffleTaskManager().requireBuffer(request.getRequireSize());
    StatusCode status = StatusCode.SUCCESS;
    if (requireBufferId == -1) {
      status = StatusCode.NO_BUFFER;
    }
    RequireBufferResponse response =
        RequireBufferResponse
            .newBuilder()
            .setStatus(valueOf(status))
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
    AppHeartBeatResponse response = AppHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(valueOf(StatusCode.SUCCESS))
        .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Cancelled by client {} for after deadline.", appId);
      return;
    }

    LOG.info("Get heartbeat from {}", appId);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleResult(ReportShuffleResultRequest request,
      StreamObserver<ReportShuffleResultResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    long taskAttemptId = request.getTaskAttemptId();
    int bitmapNum = request.getBitmapNum();
    Map<Integer, long[]> partitionToBlockIds = toPartitionBlocksMap(request.getPartitionToBlockIdsList());
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    ReportShuffleResultResponse reply;
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], taskAttemptId[" + taskAttemptId + "]";

    try {
      LOG.info("Report " + partitionToBlockIds.size() + " blocks as shuffle result for the task of " + requestInfo);
      shuffleServer.getShuffleTaskManager().addFinishedBlockIds(appId, shuffleId, partitionToBlockIds, bitmapNum);
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = "error happened when report shuffle result, check shuffle server for detail";
      LOG.error("Error happened when report shuffle result for " + requestInfo, e);
    }

    reply = ReportShuffleResultResponse.newBuilder().setStatus(valueOf(status)).setRetMsg(msg).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResult(GetShuffleResultRequest request,
      StreamObserver<GetShuffleResultResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();

    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleResultResponse reply;
    byte[] serializedBlockIds = null;
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    try {
      serializedBlockIds = shuffleServer.getShuffleTaskManager().getFinishedBlockIds(
          appId, shuffleId, partitionId);
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
      LOG.error("Error happened when get shuffle result for " + requestInfo, e);
    }

    if (serializedBlockIds == null) {
      serializedBlockIds = new byte[]{};
      status = StatusCode.INTERNAL_ERROR;
      msg = "can't find shuffle data";
      LOG.error("Error happened when get shuffle result for " + requestInfo + " because " + msg);
    }

    reply = GetShuffleResultResponse.newBuilder()
        .setStatus(valueOf(status))
        .setRetMsg(msg)
        .setSerializedBitmap(ByteString.copyFrom(serializedBlockIds))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleData(GetShuffleDataRequest request,
      StreamObserver<GetShuffleDataResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();

    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    int partitionNumPerRange = request.getPartitionNumPerRange();
    int partitionNum = request.getPartitionNum();
    int readBufferSize = request.getReadBufferSize();
    int segmentIndex = request.getSegmentIndex();
    String storageType = shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE);
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleDataResponse reply = null;
    ShuffleDataResult sdr;
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId["
        + partitionId + "]";

    if (shuffleServer.getMultiStorageManager() != null) {
      shuffleServer.getMultiStorageManager().updateLastReadTs(appId, shuffleId, partitionId);
    }

    if (shuffleServer.getShuffleBufferManager().requireReadMemoryWithRetry(readBufferSize)) {
      try {
        long start = System.currentTimeMillis();
        sdr = shuffleServer.getShuffleTaskManager().getShuffleData(appId, shuffleId, partitionId,
            partitionNumPerRange, partitionNum, readBufferSize, storageType, segmentIndex);
        long readTime = System.currentTimeMillis() - start;
        ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
        ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getData().length);
        LOG.info("Successfully getShuffleData cost " + readTime + " ms for "
            + requestInfo + " with " + sdr.getData().length + " bytes and "
            + sdr.getBufferSegments().size() + " blocks");
        reply = GetShuffleDataResponse.newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg)
            .setData(ByteString.copyFrom(sdr.getData()))
            .addAllBlockSegments(toBlockSegments(sdr.getBufferSegments()))
            .build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        reply = GetShuffleDataResponse.newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg)
            .build();
      } finally {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
      }
    } else {
      status = StatusCode.INTERNAL_ERROR;
      msg = "Can't require memory to get shuffle data";
      LOG.error(msg + " for " + requestInfo);
      reply = GetShuffleDataResponse.newBuilder()
          .setStatus(valueOf(status))
          .setRetMsg(msg)
          .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  private List<ShuffleDataBlockSegment> toBlockSegments(List<BufferSegment> bufferSegments) {
    List<ShuffleDataBlockSegment> ret = Lists.newArrayList();

    for (BufferSegment segment : bufferSegments) {
      ret.add(ShuffleDataBlockSegment.newBuilder()
          .setBlockId(segment.getBlockId())
          .setOffset(segment.getOffset())
          .setLength(segment.getLength())
          .setUncompressLength(segment.getUncompressLength())
          .setCrc(segment.getCrc())
          .setTaskAttemptId(segment.getTaskAttemptId())
          .build());
    }

    return ret;
  }

  private List<ShufflePartitionedData> toPartitionedData(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (ShuffleData data : req.getShuffleDataList()) {
      ret.add(new ShufflePartitionedData(
          data.getPartitionId(),
          toPartitionedBlock(data.getBlockList())));
    }

    return ret;
  }

  private ShufflePartitionedBlock[] toPartitionedBlock(List<ShuffleBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return new ShufflePartitionedBlock[]{};
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    int i = 0;
    for (ShuffleBlock block : blocks) {
      ret[i] = new ShufflePartitionedBlock(
          block.getLength(),
          block.getUncompressLength(),
          block.getCrc(),
          block.getBlockId(),
          block.getTaskAttemptId(),
          block.getData().toByteArray());
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

  private List<PartitionRange> toPartitionRanges(List<ShufflePartitionRange> shufflePartitionRanges) {
    List<PartitionRange> partitionRanges = Lists.newArrayList();
    for (ShufflePartitionRange spr : shufflePartitionRanges) {
      partitionRanges.add(new PartitionRange(spr.getStart(), spr.getEnd()));
    }
    return partitionRanges;
  }
}
