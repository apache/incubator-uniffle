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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
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
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);
  private String clientType;
  private int retryMax;
  private long retryIntervalMax;
  private List<CoordinatorClient> coordinatorClients = Lists.newLinkedList();
  private Set<ShuffleServerInfo> shuffleServerInfoSet = Sets.newConcurrentHashSet();
  private CoordinatorClientFactory coordinatorClientFactory;
  private ExecutorService heartBeatExecutorService;

  public ShuffleWriteClientImpl(String clientType, int retryMax, long retryIntervalMax, int heartBeatThreadNum) {
    this.clientType = clientType;
    this.retryMax = retryMax;
    this.retryIntervalMax = retryIntervalMax;
    coordinatorClientFactory = new CoordinatorClientFactory(clientType);
    heartBeatExecutorService = Executors.newFixedThreadPool(heartBeatThreadNum,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("client-heartbeat-%d").build());
  }

  private void sendShuffleDataAsync(
      String appId,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds,
      Set<Long> successBlockIds,
      Set<Long> tempFailedBlockIds) {
    if (serverToBlocks != null) {
      serverToBlocks.entrySet().parallelStream().forEach(entry -> {
        ShuffleServerInfo ssi = entry.getKey();
        try {
          RssSendShuffleDataRequest request = new RssSendShuffleDataRequest(
              appId, retryMax, retryIntervalMax, entry.getValue());
          long s = System.currentTimeMillis();
          RssSendShuffleDataResponse response = getShuffleServerClient(ssi).sendShuffleData(request);
          LOG.info("ShuffleWriteClientImpl sendShuffleData cost:" + (System.currentTimeMillis() - s));

          if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
            successBlockIds.addAll(serverToBlockIds.get(ssi));
            LOG.info("Send: " + serverToBlockIds.get(ssi).size()
                + " blocks to [" + ssi.getId() + "] successfully");
          } else {
            tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
            LOG.error("Send: " + serverToBlockIds.get(ssi).size() + " blocks to [" + ssi.getId()
                + "] failed with statusCode[" + response.getStatusCode() + "], ");
          }
        } catch (Exception e) {
          tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
          LOG.error("Send: " + serverToBlockIds.get(ssi).size() + " blocks to [" + ssi.getId() + "] failed.", e);
        }
      });
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
    shuffleServerInfoSet.parallelStream().forEach(ssi -> {
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
  public ShuffleAssignmentsInfo getShuffleAssignments(String appId, int shuffleId, int partitionNum,
      int partitionNumPerRange, int dataReplica, Set<String> requiredTags) {
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest(
        appId, shuffleId, partitionNum, partitionNumPerRange, dataReplica, requiredTags);

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
    boolean isSuccessful = true;
    Map<ShuffleServerInfo, List<Integer>> groupedPartitions = Maps.newConcurrentMap();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      for (ShuffleServerInfo ssi : entry.getValue()) {
        if (!groupedPartitions.containsKey(ssi)) {
          groupedPartitions.putIfAbsent(ssi, Lists.newArrayList());
        }
        groupedPartitions.get(ssi).add(entry.getKey());
      }
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
        } else {
          isSuccessful = false;
          LOG.warn("Report shuffle result to " + ssi + " for appId[" + appId
              + "], shuffleId[" + shuffleId + "] failed with " + response.getStatusCode());
          break;
        }
      } catch (Exception e) {
        isSuccessful = false;
        LOG.warn("Report shuffle result is failed to " + ssi
            + " for appId[" + appId + "], shuffleId[" + shuffleId + "]");
        break;
      }
    }
    if (!isSuccessful) {
      throw new RssException("Report shuffle result is failed for appId["
          + appId + "], shuffleId[" + shuffleId + "]");
    }
  }

  @Override
  public Roaring64NavigableMap getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, int partitionId) {
    RssGetShuffleResultRequest request = new RssGetShuffleResultRequest(
        appId, shuffleId, partitionId);
    boolean isSuccessful = false;
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssGetShuffleResultResponse response = ShuffleServerClientFactory
            .getInstance().getShuffleServerClient(clientType, ssi).getShuffleResult(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          blockIdBitmap = response.getBlockIdBitmap();
          isSuccessful = true;
          break;
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
    shuffleServerInfoSet.parallelStream().forEach(shuffleServerInfo -> {
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
        }
    );

    coordinatorClients.parallelStream().forEach(coordinatorClient -> {
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
        }
    );
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
  protected ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance().getShuffleServerClient(clientType, shuffleServerInfo);
  }
}
