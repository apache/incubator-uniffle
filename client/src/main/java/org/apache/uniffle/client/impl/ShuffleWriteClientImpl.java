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

package org.apache.uniffle.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssApplicationInfoRequest;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.request.RssFetchRemoteStorageRequest;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleByAppIdRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.ClientResponse;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssApplicationInfoResponse;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.client.response.RssFetchRemoteStorageResponse;
import org.apache.uniffle.client.response.RssFinishShuffleResponse;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendCommitResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleByAppIdResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);

  private String clientType;
  private int retryMax;
  private long retryIntervalMax;
  private List<CoordinatorClient> coordinatorClients = Lists.newLinkedList();
  // appId -> shuffleId -> servers
  private Map<String, Map<Integer, Set<ShuffleServerInfo>>> shuffleServerInfoMap =
      JavaUtils.newConcurrentMap();
  private CoordinatorClientFactory coordinatorClientFactory;
  private ExecutorService heartBeatExecutorService;
  private int replica;
  private int replicaWrite;
  private int replicaRead;
  private boolean replicaSkipEnabled;
  private int dataCommitPoolSize = -1;
  private final ExecutorService dataTransferPool;
  private final int unregisterThreadPoolSize;
  private final int unregisterRequestTimeSec;
  private Set<ShuffleServerInfo> defectiveServers;
  private RssConf rssConf;
  private BlockIdLayout blockIdLayout;

  public ShuffleWriteClientImpl(ShuffleClientFactory.WriteClientBuilder builder) {
    // set default value
    if (builder.getRssConf() == null) {
      builder.rssConf(new RssConf());
    }
    if (builder.getUnregisterThreadPoolSize() == 0) {
      builder.unregisterThreadPoolSize(10);
    }
    if (builder.getUnregisterRequestTimeSec() == 0) {
      builder.unregisterRequestTimeSec(10);
    }
    this.clientType = builder.getClientType();
    this.retryMax = builder.getRetryMax();
    this.retryIntervalMax = builder.getRetryIntervalMax();
    this.coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    this.heartBeatExecutorService =
        ThreadUtils.getDaemonFixedThreadPool(builder.getHeartBeatThreadNum(), "client-heartbeat");
    this.replica = builder.getReplica();
    this.replicaWrite = builder.getReplicaWrite();
    this.replicaRead = builder.getReplicaRead();
    this.replicaSkipEnabled = builder.isReplicaSkipEnabled();
    this.dataTransferPool =
        ThreadUtils.getDaemonFixedThreadPool(
            builder.getDataTransferPoolSize(), "client-data-transfer");
    this.dataCommitPoolSize = builder.getDataCommitPoolSize();
    this.unregisterThreadPoolSize = builder.getUnregisterThreadPoolSize();
    this.unregisterRequestTimeSec = builder.getUnregisterRequestTimeSec();
    if (replica > 1) {
      defectiveServers = Sets.newConcurrentHashSet();
    }
    this.rssConf = builder.getRssConf();
    this.blockIdLayout = BlockIdLayout.from(rssConf);
  }

  private boolean sendShuffleDataAsync(
      String appId,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<BlockId>> serverToBlockIds,
      Map<BlockId, AtomicInteger> blockIdsSendSuccessTracker,
      FailedBlockSendTracker failedBlockSendTracker,
      boolean allowFastFail,
      Supplier<Boolean> needCancelRequest) {

    if (serverToBlockIds == null) {
      return true;
    }

    // If one or more servers is failed, the sending is not totally successful.
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> entry :
        serverToBlocks.entrySet()) {
      CompletableFuture<Boolean> future =
          CompletableFuture.supplyAsync(
              () -> {
                if (needCancelRequest.get()) {
                  LOG.info("The upstream task has been failed. Abort this data send.");
                  return true;
                }
                ShuffleServerInfo ssi = entry.getKey();
                try {
                  Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks =
                      entry.getValue();
                  // todo: compact unnecessary blocks that reach replicaWrite
                  RssSendShuffleDataRequest request =
                      new RssSendShuffleDataRequest(
                          appId, retryMax, retryIntervalMax, shuffleIdToBlocks);
                  long s = System.currentTimeMillis();
                  RssSendShuffleDataResponse response =
                      getShuffleServerClient(ssi).sendShuffleData(request);

                  String logMsg =
                      String.format(
                          "ShuffleWriteClientImpl sendShuffleData with %s blocks to %s cost: %s(ms)",
                          serverToBlockIds.get(ssi).size(),
                          ssi.getId(),
                          System.currentTimeMillis() - s);

                  if (response.getStatusCode() == StatusCode.SUCCESS) {
                    // mark a replica of block that has been sent
                    serverToBlockIds
                        .get(ssi)
                        .forEach(
                            blockId -> blockIdsSendSuccessTracker.get(blockId).incrementAndGet());
                    if (defectiveServers != null) {
                      defectiveServers.remove(ssi);
                    }
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("{} successfully.", logMsg);
                    }
                  } else {
                    recordFailedBlocks(
                        failedBlockSendTracker, serverToBlocks, ssi, response.getStatusCode());
                    if (defectiveServers != null) {
                      defectiveServers.add(ssi);
                    }
                    LOG.warn("{}, it failed wth statusCode[{}]", logMsg, response.getStatusCode());
                    return false;
                  }
                } catch (Exception e) {
                  recordFailedBlocks(
                      failedBlockSendTracker, serverToBlocks, ssi, StatusCode.INTERNAL_ERROR);
                  if (defectiveServers != null) {
                    defectiveServers.add(ssi);
                  }
                  LOG.warn(
                      "Send: "
                          + serverToBlockIds.get(ssi).size()
                          + " blocks to ["
                          + ssi.getId()
                          + "] failed.",
                      e);
                  return false;
                }
                return true;
              },
              dataTransferPool);
      futures.add(future);
    }

    boolean result = ClientUtils.waitUntilDoneOrFail(futures, allowFastFail);
    if (!result) {
      LOG.error(
          "Some shuffle data can't be sent to shuffle-server, is fast fail: {}, cancelled task size: {}",
          allowFastFail,
          futures.size());
    }
    return result;
  }

  void recordFailedBlocks(
      FailedBlockSendTracker blockIdsSendFailTracker,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      ShuffleServerInfo shuffleServerInfo,
      StatusCode statusCode) {
    serverToBlocks.getOrDefault(shuffleServerInfo, Collections.emptyMap()).values().stream()
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMap(List::stream)
        .forEach(block -> blockIdsSendFailTracker.add(block, shuffleServerInfo, statusCode));
  }

  void genServerToBlocks(
      ShuffleBlockInfo sbi,
      List<ShuffleServerInfo> serverList,
      int replicaNum,
      Collection<ShuffleServerInfo> excludeServers,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<BlockId>> serverToBlockIds,
      boolean excludeDefectiveServers) {
    if (replicaNum <= 0) {
      return;
    }

    Stream<ShuffleServerInfo> servers;
    if (excludeDefectiveServers && CollectionUtils.isNotEmpty(defectiveServers)) {
      servers =
          Stream.concat(
              serverList.stream().filter(x -> !defectiveServers.contains(x)),
              serverList.stream().filter(defectiveServers::contains));
    } else {
      servers = serverList.stream();
    }
    if (excludeServers != null) {
      servers = servers.filter(x -> !excludeServers.contains(x));
    }

    Stream<ShuffleServerInfo> selected = servers.limit(replicaNum);
    if (excludeServers != null) {
      selected = selected.peek(excludeServers::add);
    }
    selected.forEach(
        ssi -> {
          serverToBlockIds.computeIfAbsent(ssi, id -> Lists.newArrayList()).add(sbi.getBlockId());
          serverToBlocks
              .computeIfAbsent(ssi, id -> Maps.newHashMap())
              .computeIfAbsent(sbi.getShuffleId(), id -> Maps.newHashMap())
              .computeIfAbsent(sbi.getPartitionId(), id -> Lists.newArrayList())
              .add(sbi);
        });
  }

  /** The batch of sending belongs to the same task */
  @Override
  public SendShuffleDataResult sendShuffleData(
      String appId,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {

    // shuffleServer -> shuffleId -> partitionId -> blocks
    Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>>
        primaryServerToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>>
        secondaryServerToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, List<BlockId>> primaryServerToBlockIds = Maps.newHashMap();
    Map<ShuffleServerInfo, List<BlockId>> secondaryServerToBlockIds = Maps.newHashMap();

    // send shuffle block to shuffle server
    // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId ->
    // partitionId -> blocks
    // it will be helpful to send rpc request to shuffleServer

    // In order to reduce the data to send in quorum protocol,
    // we split these blocks into two rounds: primary and secondary.
    // The primary round contains [0, replicaWrite) replicas,
    // which is minimum number when there is no sending server failures.
    // The secondary round contains [replicaWrite, replica) replicas,
    // which is minimum number when there is at most *replicaWrite - replica* sending server
    // failures.
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      List<ShuffleServerInfo> allServers = sbi.getShuffleServerInfos();
      if (replicaSkipEnabled) {
        Set<ShuffleServerInfo> excludeServers = Sets.newHashSet();
        genServerToBlocks(
            sbi,
            allServers,
            replicaWrite,
            excludeServers,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            true);
        genServerToBlocks(
            sbi,
            allServers,
            replica - replicaWrite,
            excludeServers,
            secondaryServerToBlocks,
            secondaryServerToBlockIds,
            false);
      } else {
        // When replicaSkip is disabled, we send data to all replicas within one round.
        genServerToBlocks(
            sbi,
            allServers,
            allServers.size(),
            null,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            false);
      }
    }
    /** Records the ShuffleServer that successfully or failed to send blocks */
    // we assume that most of the blocks can be sent successfully
    // so initialize the map at first without concurrency insurance
    // AtomicInteger is enough to reflect value changes in other threads
    Map<BlockId, AtomicInteger> blockIdsSendSuccessTracker = Maps.newHashMap();
    primaryServerToBlockIds
        .values()
        .forEach(
            blockList ->
                blockList.forEach(
                    block ->
                        blockIdsSendSuccessTracker.computeIfAbsent(
                            block, id -> new AtomicInteger(0))));
    secondaryServerToBlockIds
        .values()
        .forEach(
            blockList ->
                blockList.forEach(
                    block ->
                        blockIdsSendSuccessTracker.computeIfAbsent(
                            block, id -> new AtomicInteger(0))));
    FailedBlockSendTracker blockIdsSendFailTracker = new FailedBlockSendTracker();

    // sent the primary round of blocks.
    boolean isAllSuccess =
        sendShuffleDataAsync(
            appId,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            blockIdsSendSuccessTracker,
            blockIdsSendFailTracker,
            secondaryServerToBlocks.isEmpty(),
            needCancelRequest);

    // The secondary round of blocks is sent only when the primary group issues failed sending.
    // This should be infrequent.
    // Even though the secondary round may send blocks more than replicaWrite replicas,
    // we do not apply complicated skipping logic, because server crash is rare in production
    // environment.
    if (!isAllSuccess && !secondaryServerToBlocks.isEmpty() && !needCancelRequest.get()) {
      LOG.info("The sending of primary round is failed partially, so start the secondary round");
      sendShuffleDataAsync(
          appId,
          secondaryServerToBlocks,
          secondaryServerToBlockIds,
          blockIdsSendSuccessTracker,
          blockIdsSendFailTracker,
          true,
          needCancelRequest);
    }

    Set<BlockId> blockIdsSendSuccessSet = Sets.newHashSet();
    blockIdsSendSuccessTracker
        .entrySet()
        .forEach(
            successBlockId -> {
              if (successBlockId.getValue().get() >= replicaWrite) {
                blockIdsSendSuccessSet.add(successBlockId.getKey());
                // If the replicaWrite to be sent is reached,
                // no matter whether the block fails to be sent or not,
                // the block is considered to have been sent successfully and is removed from the
                // failed block tracker
                blockIdsSendFailTracker.remove(successBlockId.getKey());
              }
            });
    return new SendShuffleDataResult(blockIdsSendSuccessSet, blockIdsSendFailTracker);
  }

  /**
   * This method will wait until all shuffle data have been flushed to durable storage in assigned
   * shuffle servers.
   *
   * @param shuffleServerInfoSet
   * @param appId
   * @param shuffleId
   * @param numMaps
   * @return
   */
  @Override
  public boolean sendCommit(
      Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
    ForkJoinPool forkJoinPool =
        new ForkJoinPool(
            dataCommitPoolSize == -1 ? shuffleServerInfoSet.size() : dataCommitPoolSize);
    AtomicInteger successfulCommit = new AtomicInteger(0);
    try {
      forkJoinPool
          .submit(
              () -> {
                shuffleServerInfoSet
                    .parallelStream()
                    .forEach(
                        ssi -> {
                          RssSendCommitRequest request = new RssSendCommitRequest(appId, shuffleId);
                          String errorMsg =
                              "Failed to commit shuffle data to "
                                  + ssi
                                  + " for shuffleId["
                                  + shuffleId
                                  + "]";
                          long startTime = System.currentTimeMillis();
                          try {
                            RssSendCommitResponse response =
                                getShuffleServerClient(ssi).sendCommit(request);
                            if (response.getStatusCode() == StatusCode.SUCCESS) {
                              int commitCount = response.getCommitCount();
                              LOG.info(
                                  "Successfully sendCommit for appId["
                                      + appId
                                      + "], shuffleId["
                                      + shuffleId
                                      + "] to ShuffleServer["
                                      + ssi.getId()
                                      + "], cost "
                                      + (System.currentTimeMillis() - startTime)
                                      + " ms, got committed maps["
                                      + commitCount
                                      + "], map number of stage is "
                                      + numMaps);
                              if (commitCount >= numMaps) {
                                RssFinishShuffleResponse rfsResponse =
                                    getShuffleServerClient(ssi)
                                        .finishShuffle(
                                            new RssFinishShuffleRequest(appId, shuffleId));
                                if (rfsResponse.getStatusCode() != StatusCode.SUCCESS) {
                                  String msg =
                                      "Failed to finish shuffle to "
                                          + ssi
                                          + " for shuffleId["
                                          + shuffleId
                                          + "] with statusCode "
                                          + rfsResponse.getStatusCode();
                                  LOG.error(msg);
                                  throw new Exception(msg);
                                } else {
                                  LOG.info(
                                      "Successfully finish shuffle to "
                                          + ssi
                                          + " for shuffleId["
                                          + shuffleId
                                          + "]");
                                }
                              }
                            } else {
                              String msg =
                                  errorMsg + " with statusCode " + response.getStatusCode();
                              LOG.error(msg);
                              throw new Exception(msg);
                            }
                            successfulCommit.incrementAndGet();
                          } catch (Exception e) {
                            LOG.error(errorMsg, e);
                          }
                        });
              })
          .join();
    } finally {
      forkJoinPool.shutdownNow();
    }

    // check if every commit/finish call is successful
    return successfulCommit.get() == shuffleServerInfoSet.size();
  }

  @Override
  public void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite) {
    String user = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      LOG.error("Error on getting user from ugi.", e);
    }

    RssRegisterShuffleRequest request =
        new RssRegisterShuffleRequest(
            appId,
            shuffleId,
            partitionRanges,
            remoteStorage,
            user,
            dataDistributionType,
            maxConcurrencyPerPartitionToWrite);
    RssRegisterShuffleResponse response =
        getShuffleServerClient(shuffleServerInfo).registerShuffle(request);

    String msg =
        "Error happened when registerShuffle with appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], "
            + shuffleServerInfo;
    throwExceptionIfNecessary(response, msg);
    addShuffleServer(appId, shuffleId, shuffleServerInfo);
  }

  @Override
  public void registerCoordinators(String coordinators) {
    List<CoordinatorClient> clients =
        coordinatorClientFactory.createCoordinatorClient(
            ClientType.valueOf(this.clientType), coordinators);
    coordinatorClients.addAll(clients);
  }

  @Override
  public Map<String, String> fetchClientConf(int timeoutMs) {
    RssFetchClientConfResponse response =
        new RssFetchClientConfResponse(StatusCode.INTERNAL_ERROR, "Empty coordinator clients");
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      response = coordinatorClient.fetchClientConf(new RssFetchClientConfRequest(timeoutMs));
      if (response.getStatusCode() == StatusCode.SUCCESS) {
        LOG.info("Success to get conf from {}", coordinatorClient.getDesc());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", coordinatorClient.getDesc());
      }
    }
    return response.getClientConf();
  }

  @Override
  public RemoteStorageInfo fetchRemoteStorage(String appId) {
    RemoteStorageInfo remoteStorage = new RemoteStorageInfo("");
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      RssFetchRemoteStorageResponse response =
          coordinatorClient.fetchRemoteStorage(new RssFetchRemoteStorageRequest(appId));
      if (response.getStatusCode() == StatusCode.SUCCESS) {
        remoteStorage = response.getRemoteStorageInfo();
        LOG.info("Success to get storage {} from {}", remoteStorage, coordinatorClient.getDesc());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", coordinatorClient.getDesc());
      }
    }
    return remoteStorage;
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency) {
    return getShuffleAssignments(
        appId,
        shuffleId,
        partitionNum,
        partitionNumPerRange,
        requiredTags,
        assignmentShuffleServerNumber,
        estimateTaskConcurrency,
        Sets.newConcurrentHashSet());
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds) {
    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            appId,
            shuffleId,
            partitionNum,
            partitionNumPerRange,
            replica,
            requiredTags,
            assignmentShuffleServerNumber,
            estimateTaskConcurrency,
            faultyServerIds);

    RssGetShuffleAssignmentsResponse response =
        new RssGetShuffleAssignmentsResponse(StatusCode.INTERNAL_ERROR);
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      try {
        response = coordinatorClient.getShuffleAssignments(request);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }

      if (response.getStatusCode() == StatusCode.SUCCESS) {
        LOG.info("Success to get shuffle server assignment from {}", coordinatorClient.getDesc());
        break;
      }
    }
    String msg =
        "Error happened when getShuffleAssignments with appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], numMaps["
            + partitionNum
            + "], partitionNumPerRange["
            + partitionNumPerRange
            + "] to coordinator. "
            + "Error message: "
            + response.getMessage();
    throwExceptionIfNecessary(response, msg);

    return new ShuffleAssignmentsInfo(
        response.getPartitionToServers(), response.getServerToPartitionRanges());
  }

  @Override
  public void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<BlockId>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum) {
    // record blockId count for quora check,but this is not a good realization.
    Map<BlockId, Integer> blockReportTracker =
        createBlockReportTracker(serverToPartitionToBlockIds);
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Set<BlockId>>> entry :
        serverToPartitionToBlockIds.entrySet()) {
      Map<Integer, Set<BlockId>> requestBlockIds = entry.getValue();
      if (requestBlockIds.isEmpty()) {
        continue;
      }
      RssReportShuffleResultRequest request =
          new RssReportShuffleResultRequest(
              appId,
              shuffleId,
              taskAttemptId,
              requestBlockIds.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))),
              bitmapNum);
      ShuffleServerInfo ssi = entry.getKey();
      try {
        RssReportShuffleResultResponse response =
            getShuffleServerClient(ssi).reportShuffleResult(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          LOG.info(
              "Report shuffle result to "
                  + ssi
                  + " for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "] successfully");
        } else {
          LOG.warn(
              "Report shuffle result to "
                  + ssi
                  + " for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "] failed with "
                  + response.getStatusCode());
          recordFailedBlockIds(blockReportTracker, requestBlockIds);
        }
      } catch (Exception e) {
        LOG.warn(
            "Report shuffle result is failed to "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]");
        recordFailedBlockIds(blockReportTracker, requestBlockIds);
      }
    }
    if (blockReportTracker.values().stream().anyMatch(cnt -> cnt < replicaWrite)) {
      throw new RssException(
          "Quorum check of report shuffle result is failed for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "]");
    }
  }

  private void recordFailedBlockIds(
      Map<BlockId, Integer> blockReportTracker, Map<Integer, Set<BlockId>> requestBlockIds) {
    requestBlockIds.values().stream()
        .flatMap(Set::stream)
        .forEach(blockId -> blockReportTracker.merge(blockId, -1, Integer::sum));
  }

  private Map<BlockId, Integer> createBlockReportTracker(
      Map<ShuffleServerInfo, Map<Integer, Set<BlockId>>> serverToPartitionToBlockIds) {
    Map<BlockId, Integer> blockIdCount = new HashMap<>();
    for (Map<Integer, Set<BlockId>> partitionToBlockIds : serverToPartitionToBlockIds.values()) {
      for (Set<BlockId> blockIds : partitionToBlockIds.values()) {
        for (BlockId blockId : blockIds) {
          blockIdCount.put(blockId, blockIdCount.getOrDefault(blockId, 0) + 1);
        }
      }
    }
    return blockIdCount;
  }

  @Override
  public BlockIdSet getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId) {
    RssGetShuffleResultRequest request =
        new RssGetShuffleResultRequest(appId, shuffleId, partitionId, blockIdLayout);
    boolean isSuccessful = false;
    BlockIdSet blockIdBitmap = BlockIdSet.empty();
    int successCnt = 0;
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssGetShuffleResultResponse response =
            getShuffleServerClient(ssi).getShuffleResult(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          // merge into blockIds from multiple servers.
          BlockIdSet blockIdBitmapOfServer = response.getBlockIdBitmap();
          blockIdBitmap.addAll(blockIdBitmapOfServer);
          successCnt++;
          if (successCnt >= replicaRead) {
            isSuccessful = true;
            break;
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "Get shuffle result is failed from "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]");
      }
    }
    if (!isSuccessful) {
      throw new RssFetchFailedException(
          "Get shuffle result is failed for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIdBitmap;
  }

  @Override
  public BlockIdSet getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    BlockIdSet blockIdBitmap = BlockIdSet.empty();
    Set<Integer> allRequestedPartitionIds = new HashSet<>();
    for (Map.Entry<ShuffleServerInfo, Set<Integer>> entry : serverToPartitions.entrySet()) {
      ShuffleServerInfo shuffleServerInfo = entry.getKey();
      Set<Integer> requestPartitions = Sets.newHashSet();
      for (Integer partitionId : entry.getValue()) {
        if (!replicaRequirementTracking.isSatisfied(partitionId, replicaRead)) {
          requestPartitions.add(partitionId);
        }
      }
      allRequestedPartitionIds.addAll(requestPartitions);
      RssGetShuffleResultForMultiPartRequest request =
          new RssGetShuffleResultForMultiPartRequest(
              appId, shuffleId, requestPartitions, blockIdLayout);
      try {
        RssGetShuffleResultResponse response =
            getShuffleServerClient(shuffleServerInfo).getShuffleResultForMultiPart(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          // merge into blockIds from multiple servers.
          BlockIdSet blockIdBitmapOfServer = response.getBlockIdBitmap();
          blockIdBitmap.addAll(blockIdBitmapOfServer);
          for (Integer partitionId : requestPartitions) {
            replicaRequirementTracking.markPartitionOfServerSuccessful(
                partitionId, shuffleServerInfo);
          }
        }
      } catch (Exception e) {
        failedPartitions.addAll(requestPartitions);
        LOG.warn(
            "Get shuffle result is failed from "
                + shuffleServerInfo
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], requestPartitions"
                + requestPartitions,
            e);
      }
    }
    boolean isSuccessful =
        allRequestedPartitionIds.stream()
            .allMatch(x -> replicaRequirementTracking.isSatisfied(x, replicaRead));
    if (!isSuccessful) {
      LOG.error("Failed to meet replica requirement: {}", replicaRequirementTracking);
      throw new RssFetchFailedException(
          "Get shuffle result is failed for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIdBitmap;
  }

  @Override
  public void registerApplicationInfo(String appId, long timeoutMs, String user) {
    RssApplicationInfoRequest request = new RssApplicationInfoRequest(appId, timeoutMs, user);

    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        coordinatorClient -> {
          try {
            RssApplicationInfoResponse response =
                coordinatorClient.registerApplicationInfo(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.error("Failed to send applicationInfo to " + coordinatorClient.getDesc());
            } else {
              LOG.info("Successfully send applicationInfo to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn(
                "Error happened when send applicationInfo to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        timeoutMs,
        "register application");
  }

  @Override
  public void sendAppHeartbeat(String appId, long timeoutMs) {
    RssAppHeartBeatRequest request = new RssAppHeartBeatRequest(appId, timeoutMs);
    Set<ShuffleServerInfo> allShuffleServers = getAllShuffleServers(appId);

    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        allShuffleServers,
        shuffleServerInfo -> {
          try {
            ShuffleServerClient client =
                ShuffleServerClientFactory.getInstance()
                    .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
            RssAppHeartBeatResponse response = client.sendHeartBeat(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.warn("Failed to send heartbeat to " + shuffleServerInfo);
            }
          } catch (Exception e) {
            LOG.warn("Error happened when send heartbeat to " + shuffleServerInfo, e);
          }
          return null;
        },
        timeoutMs,
        "send heartbeat to shuffle server");

    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        coordinatorClient -> {
          try {
            RssAppHeartBeatResponse response = coordinatorClient.sendAppHeartBeat(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.warn("Failed to send heartbeat to " + coordinatorClient.getDesc());
            } else {
              LOG.info("Successfully send heartbeat to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn("Error happened when send heartbeat to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        timeoutMs,
        "send heartbeat to coordinator");
  }

  @Override
  public void close() {
    heartBeatExecutorService.shutdownNow();
    coordinatorClients.forEach(CoordinatorClient::close);
    dataTransferPool.shutdownNow();
  }

  @Override
  public void unregisterShuffle(String appId, int shuffleId) {
    RssUnregisterShuffleRequest request = new RssUnregisterShuffleRequest(appId, shuffleId);

    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return;
    }
    Set<ShuffleServerInfo> shuffleServerInfos = appServerMap.get(shuffleId);
    if (shuffleServerInfos == null) {
      return;
    }

    ExecutorService executorService = null;
    try {
      executorService =
          ThreadUtils.getDaemonFixedThreadPool(
              Math.min(unregisterThreadPoolSize, shuffleServerInfos.size()), "unregister-shuffle");

      ThreadUtils.executeTasks(
          executorService,
          shuffleServerInfos,
          shuffleServerInfo -> {
            try {
              ShuffleServerClient client =
                  ShuffleServerClientFactory.getInstance()
                      .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
              RssUnregisterShuffleResponse response = client.unregisterShuffle(request);
              if (response.getStatusCode() != StatusCode.SUCCESS) {
                LOG.warn("Failed to unregister shuffle to " + shuffleServerInfo);
              }
            } catch (Exception e) {
              LOG.warn("Error happened when unregistering to " + shuffleServerInfo, e);
            }
            return null;
          },
          unregisterRequestTimeSec,
          "unregister shuffle server");

    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
      removeShuffleServer(appId, shuffleId);
    }
  }

  @Override
  public void unregisterShuffle(String appId) {
    RssUnregisterShuffleByAppIdRequest request = new RssUnregisterShuffleByAppIdRequest(appId);

    if (appId == null) {
      return;
    }
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return;
    }
    Set<ShuffleServerInfo> shuffleServerInfos = getAllShuffleServers(appId);

    ExecutorService executorService = null;
    try {
      executorService =
          ThreadUtils.getDaemonFixedThreadPool(
              Math.min(unregisterThreadPoolSize, shuffleServerInfos.size()), "unregister-shuffle");

      ThreadUtils.executeTasks(
          executorService,
          shuffleServerInfos,
          shuffleServerInfo -> {
            try {
              ShuffleServerClient client =
                  ShuffleServerClientFactory.getInstance()
                      .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
              RssUnregisterShuffleByAppIdResponse response =
                  client.unregisterShuffleByAppId(request);
              if (response.getStatusCode() != StatusCode.SUCCESS) {
                LOG.warn("Failed to unregister shuffle to " + shuffleServerInfo);
              }
            } catch (Exception e) {
              LOG.warn("Error happened when unregistering to " + shuffleServerInfo, e);
            }
            return null;
          },
          unregisterRequestTimeSec,
          "unregister shuffle server");

    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
      shuffleServerInfoMap.remove(appId);
    }
  }

  private void throwExceptionIfNecessary(ClientResponse response, String errorMsg) {
    if (response != null && response.getStatusCode() != StatusCode.SUCCESS) {
      LOG.error(errorMsg);
      throw new RssException(errorMsg);
    }
  }

  Set<ShuffleServerInfo> getAllShuffleServers(String appId) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return Collections.emptySet();
    }
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    appServerMap.values().forEach(serverInfos::addAll);
    return serverInfos;
  }

  @VisibleForTesting
  public ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance()
        .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
  }

  @VisibleForTesting
  Set<ShuffleServerInfo> getDefectiveServers() {
    return defectiveServers;
  }

  void addShuffleServer(String appId, int shuffleId, ShuffleServerInfo serverInfo) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      appServerMap = JavaUtils.newConcurrentMap();
      shuffleServerInfoMap.put(appId, appServerMap);
    }
    Set<ShuffleServerInfo> shuffleServerInfos = appServerMap.get(shuffleId);
    if (shuffleServerInfos == null) {
      shuffleServerInfos = Sets.newConcurrentHashSet();
      appServerMap.put(shuffleId, shuffleServerInfos);
    }
    shuffleServerInfos.add(serverInfo);
  }

  @VisibleForTesting
  void removeShuffleServer(String appId, int shuffleId) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap != null) {
      appServerMap.remove(shuffleId);
    }
  }
}
