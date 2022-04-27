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

package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.RssProtos.AccessClusterRequest;
import com.tencent.rss.proto.RssProtos.AccessClusterResponse;
import com.tencent.rss.proto.RssProtos.AppHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.AppHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse;
import com.tencent.rss.proto.RssProtos.ClientConfItem;
import com.tencent.rss.proto.RssProtos.FetchClientConfResponse;
import com.tencent.rss.proto.RssProtos.FetchRemoteStorageRequest;
import com.tencent.rss.proto.RssProtos.FetchRemoteStorageResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorGrpcService extends CoordinatorServerGrpc.CoordinatorServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcService.class);

  private final CoordinatorServer coordinatorServer;

  CoordinatorGrpcService(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
  }

  @Override
  public void getShuffleServerList(
      Empty request,
      StreamObserver<GetShuffleServerListResponse> responseObserver) {
    final GetShuffleServerListResponse response = GetShuffleServerListResponse
        .newBuilder()
        .addAllServers(
            coordinatorServer
                .getClusterManager()
                .list().stream()
                .map(ServerNode::convertToGrpcProto)
                .collect(Collectors.toList()))
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleServerNum(
      Empty request,
      StreamObserver<GetShuffleServerNumResponse> responseObserver) {
    final int num = coordinatorServer.getClusterManager().getNodesNum();
    final GetShuffleServerNumResponse response = GetShuffleServerNumResponse
        .newBuilder()
        .setNum(num)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleAssignments(
      GetShuffleServerRequest request,
      StreamObserver<GetShuffleAssignmentsResponse> responseObserver) {
    final String appId = request.getApplicationId();
    final int shuffleId = request.getShuffleId();
    final int partitionNum = request.getPartitionNum();
    final int partitionNumPerRange = request.getPartitionNumPerRange();
    final int replica = request.getDataReplica();
    final Set<String> requiredTags = Sets.newHashSet(request.getRequireTagsList());

    LOG.info("Request of getShuffleAssignments for appId[" + appId
        + "], shuffleId[" + shuffleId + "], partitionNum[" + partitionNum
        + "], partitionNumPerRange[" + partitionNumPerRange + "], replica[" + replica + "]");

    GetShuffleAssignmentsResponse response;
    try {
      final PartitionRangeAssignment pra =
          coordinatorServer.getAssignmentStrategy().assign(partitionNum, partitionNumPerRange, replica, requiredTags);
      response =
          CoordinatorUtils.toGetShuffleAssignmentsResponse(pra);
      logAssignmentResult(appId, shuffleId, pra);
      responseObserver.onNext(response);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      response = GetShuffleAssignmentsResponse.newBuilder().setStatus(StatusCode.INTERNAL_ERROR).build();
      responseObserver.onNext(response);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void heartbeat(
      ShuffleServerHeartBeatRequest request,
      StreamObserver<ShuffleServerHeartBeatResponse> responseObserver) {
    final ServerNode serverNode = toServerNode(request);
    coordinatorServer.getClusterManager().add(serverNode);
    final ShuffleServerHeartBeatResponse response = ShuffleServerHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    LOG.debug("Got heartbeat from " + serverNode);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void checkServiceAvailable(
      Empty request,
      StreamObserver<CheckServiceAvailableResponse> responseObserver) {
    final CheckServiceAvailableResponse response = CheckServiceAvailableResponse
        .newBuilder()
        .setAvailable(coordinatorServer.getClusterManager().getNodesNum() > 0)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void reportClientOperation(
      ReportShuffleClientOpRequest request,
      StreamObserver<ReportShuffleClientOpResponse> responseObserver) {
    final String clientHost = request.getClientHost();
    final int clientPort = request.getClientPort();
    final ShuffleServerId shuffleServer = request.getServer();
    final String operation = request.getOperation();
    LOG.info(clientHost + ":" + clientPort + "->" + operation + "->" + shuffleServer);
    final ReportShuffleClientOpResponse response = ReportShuffleClientOpResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void appHeartbeat(
      AppHeartBeatRequest request,
      StreamObserver<AppHeartBeatResponse> responseObserver) {
    String appId = request.getAppId();
    coordinatorServer.getApplicationManager().refreshAppId(appId);
    LOG.debug("Got heartbeat from application: " + appId);
    AppHeartBeatResponse response = AppHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Cancelled by client {} for after deadline.", appId);
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void accessCluster(AccessClusterRequest request, StreamObserver<AccessClusterResponse> responseObserver) {
    StatusCode statusCode = StatusCode.SUCCESS;
    AccessClusterResponse response;
    AccessManager accessManager = coordinatorServer.getAccessManager();

    AccessInfo accessInfo = new AccessInfo(request.getAccessId(), Sets.newHashSet(request.getTagsList()));
    AccessCheckResult result = accessManager.handleAccessRequest(accessInfo);
    if (!result.isSuccess()) {
      statusCode = StatusCode.ACCESS_DENIED;
    }

    response = AccessClusterResponse
        .newBuilder()
        .setStatus(statusCode)
        .setRetMsg(result.getMsg())
        .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Cancelled by client {} for after deadline.", accessInfo);
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void fetchClientConf(Empty empty, StreamObserver<FetchClientConfResponse> responseObserver) {
    FetchClientConfResponse response;
    FetchClientConfResponse.Builder builder = FetchClientConfResponse.newBuilder().setStatus(StatusCode.SUCCESS);
    boolean dynamicConfEnabled = coordinatorServer.getCoordinatorConf().getBoolean(
        CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED);
    if (dynamicConfEnabled) {
      ClientConfManager clientConfManager = coordinatorServer.getClientConfManager();
      for (Map.Entry<String, String> kv : clientConfManager.getClientConf().entrySet()) {
        builder.addClientConf(
            ClientConfItem.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()).build());
      }
    }
    response = builder.build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Fetch client conf cancelled by client for after deadline.");
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void fetchRemoteStorage(
      FetchRemoteStorageRequest request,
      StreamObserver<FetchRemoteStorageResponse> responseObserver) {
    FetchRemoteStorageResponse response;
    StatusCode status = StatusCode.SUCCESS;
    String remoteStorage = "";
    String appId = request.getAppId();
    try {
      remoteStorage = coordinatorServer.getApplicationManager().pickRemoteStoragePath(appId);
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      LOG.error("Error happened when get remote storage for appId[" + appId + "]", e);
    }

    response = FetchRemoteStorageResponse.newBuilder()
        .setRemoteStorage(remoteStorage)
        .setStatus(status)
        .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Fetch client conf cancelled by client for after deadline.");
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private void logAssignmentResult(String appId, int shuffleId, PartitionRangeAssignment pra) {
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    if (assignments != null) {
      Set<String> nodeIds = Sets.newHashSet();
      for (Map.Entry<PartitionRange, List<ServerNode>> entry : assignments.entrySet()) {
        for (ServerNode node : entry.getValue()) {
          nodeIds.add(node.getId());
        }
      }
      if (!nodeIds.isEmpty()) {
        LOG.info("Shuffle Servers of assignment for appId[" + appId + "], shuffleId["
            + shuffleId + "] are " + nodeIds);
      }
    }
  }

  private ServerNode toServerNode(ShuffleServerHeartBeatRequest request) {
    boolean isHealthy = true;
    if (request.hasIsHealthy()) {
      isHealthy = request.getIsHealthy().getValue();
    }
    return new ServerNode(request.getServerId().getId(),
        request.getServerId().getIp(),
        request.getServerId().getPort(),
        request.getUsedMemory(),
        request.getPreAllocatedMemory(),
        request.getAvailableMemory(),
        request.getEventNumInFlush(),
        Sets.newHashSet(request.getTagsList()),
        isHealthy);
  }
}
