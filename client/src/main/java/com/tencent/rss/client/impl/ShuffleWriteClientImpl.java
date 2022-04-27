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

package com.tencent.rss.client.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFetchClientConfRequest;
import com.tencent.rss.client.request.RssFetchRemoteStorageRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.ClientResponse;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssFetchClientConfResponse;
import com.tencent.rss.client.response.RssFetchRemoteStorageResponse;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);
  private String clientType;
  private int retryMax;
  private long retryIntervalMax;
  private List<CoordinatorClient> coordinatorClients = Lists.newLinkedList();
  private Set<ShuffleServerInfo> shuffleServerInfoSet = Sets.newConcurrentHashSet();
  private CoordinatorClientFactory coordinatorClientFactory;
  private ExecutorService heartBeatExecutorService;
  private int replica;
  private int replicaWrite;
  private int replicaRead;

  public ShuffleWriteClientImpl(String clientType, int retryMax, long retryIntervalMax, int heartBeatThreadNum,
                                int replica, int replicaWrite, int replicaRead) {
    this.clientType = clientType;
    this.retryMax = retryMax;
    this.retryIntervalMax = retryIntervalMax;
    coordinatorClientFactory = new CoordinatorClientFactory(clientType);
    heartBeatExecutorService = Executors.newFixedThreadPool(heartBeatThreadNum,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("client-heartbeat-%d").build());
    this.replica = replica;
    this.replicaWrite = replicaWrite;
    this.replicaRead = replicaRead;
  }

  private void sendShuffleDataAsync(
      String appId,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds,
      Set<Long> successBlockIds,
      Set<Long> tempFailedBlockIds) {

    // maintain the count of blocks that have been sent to the server
    Map<Long, AtomicInteger> blockIdsTracker = Maps.newConcurrentMap();
    serverToBlockIds.values().forEach(
      blockList -> blockList.forEach(block -> blockIdsTracker.put(block, new AtomicInteger(0)))
    );

    if (serverToBlocks != null) {
      serverToBlocks.entrySet().parallelStream().forEach(entry -> {
        ShuffleServerInfo ssi = entry.getKey();
        try {
          Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = entry.getValue();
          // todo: compact unnecessary blocks that reach replicaWrite
          RssSendShuffleDataRequest request = new RssSendShuffleDataRequest(
              appId, retryMax, retryIntervalMax, shuffleIdToBlocks);
          long s = System.currentTimeMillis();
          RssSendShuffleDataResponse response = getShuffleServerClient(ssi).sendShuffleData(request);
          LOG.info("ShuffleWriteClientImpl sendShuffleData cost:" + (System.currentTimeMillis() - s));

          if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
            // mark a replica of block that has been sent
            serverToBlockIds.get(ssi).forEach(block -> blockIdsTracker.get(block).incrementAndGet());
            LOG.info("Send: " + serverToBlockIds.get(ssi).size()
                + " blocks to [" + ssi.getId() + "] successfully");
          } else {
            LOG.warn("Send: " + serverToBlockIds.get(ssi).size() + " blocks to [" + ssi.getId()
                + "] failed with statusCode[" + response.getStatusCode() + "], ");
          }
        } catch (Exception e) {
          LOG.warn("Send: " + serverToBlockIds.get(ssi).size() + " blocks to [" + ssi.getId() + "] failed.", e);
        }
      });

      // check success and failed blocks according to the replicaWrite
      blockIdsTracker.entrySet().forEach(blockCt -> {
          long blockId = blockCt.getKey();
          int count = blockCt.getValue().get();
          if (count >= replicaWrite) {
            successBlockIds.add(blockId);
          } else {
            tempFailedBlockIds.add(blockId);
          }
        }
      );
    }
  }

  @Override
  public SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList) {

    // shuffleServer -> shuffleId -> partitionId -> blocks
    Map<ShuffleServerInfo, Map<Integer,
        Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, List<Long>> serverToBlockIds = Maps.newHashMap();
    // send shuffle block to shuffle server
    // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId -> partitionId -> blocks
    // it will be helpful to send rpc request to shuffleServer
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      int partitionId = sbi.getPartitionId();
      int shuffleId = sbi.getShuffleId();
      for (ShuffleServerInfo ssi : sbi.getShuffleServerInfos()) {
        if (!serverToBlockIds.containsKey(ssi)) {
          serverToBlockIds.put(ssi, Lists.newArrayList());
        }
        serverToBlockIds.get(ssi).add(sbi.getBlockId());

        if (!serverToBlocks.containsKey(ssi)) {
          serverToBlocks.put(ssi, Maps.newHashMap());
        }
        Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = serverToBlocks.get(ssi);
        if (!shuffleIdToBlocks.containsKey(shuffleId)) {
          shuffleIdToBlocks.put(shuffleId, Maps.newHashMap());
        }

        Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = shuffleIdToBlocks.get(shuffleId);
        if (!partitionToBlocks.containsKey(partitionId)) {
          partitionToBlocks.put(partitionId, Lists.newArrayList());
        }
        partitionToBlocks.get(partitionId).add(sbi);
      }
    }

    Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    // if send block failed, the task will fail
    // todo: better to have fallback solution when send to multiple servers
    sendShuffleDataAsync(appId, serverToBlocks, serverToBlockIds, successBlockIds, failedBlockIds);
    return new SendShuffleDataResult(successBlockIds, failedBlockIds);
  }

  @Override
  public boolean sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
    AtomicInteger successfulCommit = new AtomicInteger(0);
    shuffleServerInfoSet.stream().forEach(ssi -> {
      RssSendCommitRequest request = new RssSendCommitRequest(appId, shuffleId);
      String errorMsg = "Failed to commit shuffle data to " + ssi + " for shuffleId[" + shuffleId + "]";
      long startTime = System.currentTimeMillis();
      try {
        RssSendCommitResponse response = getShuffleServerClient(ssi).sendCommit(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          int commitCount = response.getCommitCount();
          LOG.info("Successfully sendCommit for appId[" + appId + "], shuffleId[" + shuffleId
              + "] to ShuffleServer[" + ssi.getId() + "], cost "
              + (System.currentTimeMillis() - startTime) + " ms, got committed maps["
              + commitCount + "], map number of stage is " + numMaps);
          if (commitCount >= numMaps) {
            RssFinishShuffleResponse rfsResponse =
                getShuffleServerClient(ssi).finishShuffle(new RssFinishShuffleRequest(appId, shuffleId));
            if (rfsResponse.getStatusCode() != ResponseStatusCode.SUCCESS) {
              String msg = "Failed to finish shuffle to " + ssi + " for shuffleId[" + shuffleId
                  + "] with statusCode " + rfsResponse.getStatusCode();
              LOG.error(msg);
              throw new Exception(msg);
            } else {
              LOG.info("Successfully finish shuffle to " + ssi + " for shuffleId[" + shuffleId + "]");
            }
          }
        } else {
          String msg = errorMsg + " with statusCode " + response.getStatusCode();
          LOG.error(msg);
          throw new Exception(msg);
        }
        successfulCommit.incrementAndGet();
      } catch (Exception e) {
        LOG.error(errorMsg, e);
      }
    });
    // check if every commit/finish call is successful
    return successfulCommit.get() == shuffleServerInfoSet.size();
  }

  @Override
  public void registerShuffle(ShuffleServerInfo shuffleServerInfo,
      String appId, int shuffleId, List<PartitionRange> partitionRanges) {
    RssRegisterShuffleRequest request = new RssRegisterShuffleRequest(appId, shuffleId, partitionRanges);
    RssRegisterShuffleResponse response = getShuffleServerClient(shuffleServerInfo).registerShuffle(request);

    String msg = "Error happened when registerShuffle with appId[" + appId + "], shuffleId[" + shuffleId
        + "], " + shuffleServerInfo;
    throwExceptionIfNecessary(response, msg);
    shuffleServerInfoSet.add(shuffleServerInfo);
  }

  @Override
  public void registerCoordinators(String coordinators) {
    List<CoordinatorClient> clients = coordinatorClientFactory.createCoordinatorClient(coordinators);
    coordinatorClients.addAll(clients);
  }

  @Override
  public Map<String, String> fetchClientConf(int timeoutMs) {
    RssFetchClientConfResponse response =
        new RssFetchClientConfResponse(ResponseStatusCode.INTERNAL_ERROR, "Empty coordinator clients");
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      response = coordinatorClient.fetchClientConf(new RssFetchClientConfRequest(timeoutMs));
      if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
        LOG.info("Success to get conf from {}", coordinatorClient.getDesc());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", coordinatorClient.getDesc());
      }
    }
    return response.getClientConf();
  }

  @Override
  public String fetchRemoteStorage(String appId) {
    String remoteStorage = "";
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      RssFetchRemoteStorageResponse response =
          coordinatorClient.fetchRemoteStorage(new RssFetchRemoteStorageRequest(appId));
      if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
        remoteStorage = response.getRemoteStorage();
        LOG.info("Success to get storage {} from {}", remoteStorage, coordinatorClient.getDesc());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", coordinatorClient.getDesc());
      }
    }
    return remoteStorage;
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(String appId, int shuffleId, int partitionNum,
      int partitionNumPerRange, Set<String> requiredTags) {
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest(
        appId, shuffleId, partitionNum, partitionNumPerRange, replica, requiredTags);

    RssGetShuffleAssignmentsResponse response = new RssGetShuffleAssignmentsResponse(ResponseStatusCode.INTERNAL_ERROR);
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      try {
        response = coordinatorClient.getShuffleAssignments(request);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }

      if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
        LOG.info("Success to get shuffle server assignment from {}", coordinatorClient.getDesc());
        break;
      }
    }
    String msg = "Error happened when getShuffleAssignments with appId[" + appId + "], shuffleId[" + shuffleId
        + "], numMaps[" + partitionNum + "], partitionNumPerRange[" + partitionNumPerRange + "] to coordinator";
    throwExceptionIfNecessary(response, msg);

    return new ShuffleAssignmentsInfo(response.getPartitionToServers(), response.getServerToPartitionRanges());
  }

  @Override
  public void reportShuffleResult(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      String appId,
      int shuffleId,
      long taskAttemptId,
      Map<Integer, List<Long>> partitionToBlockIds,
      int bitmapNum) {
    Map<ShuffleServerInfo, List<Integer>> groupedPartitions = Maps.newConcurrentMap();
    Map<Integer, Integer> partitionReportTracker = Maps.newConcurrentMap();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      for (ShuffleServerInfo ssi : entry.getValue()) {
        if (!groupedPartitions.containsKey(ssi)) {
          groupedPartitions.putIfAbsent(ssi, Lists.newArrayList());
        }
        groupedPartitions.get(ssi).add(entry.getKey());
      }
      partitionReportTracker.putIfAbsent(entry.getKey(), 0);
    }
    for (Map.Entry<ShuffleServerInfo, List<Integer>> entry : groupedPartitions.entrySet()) {
      Map<Integer, List<Long>> requestBlockIds = Maps.newHashMap();
      for (Integer partitionId : entry.getValue()) {
        requestBlockIds.put(partitionId, partitionToBlockIds.get(partitionId));
      }
      RssReportShuffleResultRequest request = new RssReportShuffleResultRequest(
          appId, shuffleId, taskAttemptId, requestBlockIds, bitmapNum);
      ShuffleServerInfo ssi = entry.getKey();
      try {
        RssReportShuffleResultResponse response = getShuffleServerClient(ssi).reportShuffleResult(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          LOG.info("Report shuffle result to " + ssi + " for appId[" + appId
              + "], shuffleId[" + shuffleId + "] successfully");
          for (Integer partitionId : entry.getValue()) {
            partitionReportTracker.put(partitionId, partitionReportTracker.get(partitionId) + 1);
          }
        } else {
          LOG.warn("Report shuffle result to " + ssi + " for appId[" + appId
              + "], shuffleId[" + shuffleId + "] failed with " + response.getStatusCode());
        }
      } catch (Exception e) {
        LOG.warn("Report shuffle result is failed to " + ssi
            + " for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      }
    }
    // quorum check
    for (Map.Entry<Integer, Integer> entry: partitionReportTracker.entrySet()) {
      if (entry.getValue() < replicaWrite) {
        throw new RssException("Quorum check of report shuffle result is failed for appId["
          + appId + "], shuffleId[" + shuffleId + "]");
      }
    }
  }

  @Override
  public Roaring64NavigableMap getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, int partitionId) {
    RssGetShuffleResultRequest request = new RssGetShuffleResultRequest(
        appId, shuffleId, partitionId);
    boolean isSuccessful = false;
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    int successCnt = 0;
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssGetShuffleResultResponse response = ShuffleServerClientFactory
            .getInstance().getShuffleServerClient(clientType, ssi).getShuffleResult(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          // merge into blockIds from multiple servers.
          Roaring64NavigableMap blockIdBitmapOfServer = response.getBlockIdBitmap();
          blockIdBitmap.or(blockIdBitmapOfServer);
          successCnt++;
          if (successCnt >= replicaRead) {
            isSuccessful = true;
            break;
          }
        }
      } catch (Exception e) {
        LOG.warn("Get shuffle result is failed from " + ssi
            + " for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      }
    }
    if (!isSuccessful) {
      throw new RssException("Get shuffle result is failed for appId["
          + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIdBitmap;
  }

  @Override
  public void sendAppHeartbeat(String appId, long timeoutMs) {
    RssAppHeartBeatRequest request = new RssAppHeartBeatRequest(appId, timeoutMs);
    List<Callable<Void>> callableList = Lists.newArrayList();
    shuffleServerInfoSet.stream().forEach(shuffleServerInfo -> {
          callableList.add(() -> {
            try {
              ShuffleServerClient client =
                  ShuffleServerClientFactory.getInstance().getShuffleServerClient(clientType, shuffleServerInfo);
              RssAppHeartBeatResponse response = client.sendHeartBeat(request);
              if (response.getStatusCode() != ResponseStatusCode.SUCCESS) {
                LOG.warn("Failed to send heartbeat to " + shuffleServerInfo);
              }
            } catch (Exception e) {
              LOG.warn("Error happened when send heartbeat to " + shuffleServerInfo, e);
            }
            return null;
          });
        }
    );

    coordinatorClients.stream().forEach(coordinatorClient -> {
      callableList.add(() -> {
        try {
          RssAppHeartBeatResponse response = coordinatorClient.sendAppHeartBeat(request);
          if (response.getStatusCode() != ResponseStatusCode.SUCCESS) {
            LOG.warn("Failed to send heartbeat to " + coordinatorClient.getDesc());
          } else {
            LOG.info("Successfully send heartbeat to " + coordinatorClient.getDesc());
          }
        } catch (Exception e) {
          LOG.warn("Error happened when send heartbeat to " + coordinatorClient.getDesc(), e);
        }
        return null;
      });
    });
    try {
      List<Future<Void>> futures = heartBeatExecutorService.invokeAll(callableList, timeoutMs, TimeUnit.MILLISECONDS);
      for (Future<Void> future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    } catch (InterruptedException ie) {
      LOG.warn("heartbeat is interrupted", ie);
    }
  }

  @Override
  public void close() {
    heartBeatExecutorService.shutdownNow();
    coordinatorClients.forEach(CoordinatorClient::close);
  }

  private void throwExceptionIfNecessary(ClientResponse response, String errorMsg) {
    if (response != null && response.getStatusCode() != ResponseStatusCode.SUCCESS) {
      LOG.error(errorMsg);
      throw new RssException(errorMsg);
    }
  }

  @VisibleForTesting
  public ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance().getShuffleServerClient(clientType, shuffleServerInfo);
  }

}
