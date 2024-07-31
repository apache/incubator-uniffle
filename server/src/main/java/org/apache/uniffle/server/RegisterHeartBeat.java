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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.request.RssSendHeartBeatRequest;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.proto.RssProtos;

public class RegisterHeartBeat {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterHeartBeat.class);

  private final long heartBeatInitialDelay;
  private final long heartBeatInterval;
  private final ShuffleServer shuffleServer;
  private final String coordinatorQuorum;
  private final List<CoordinatorClient> coordinatorClients;
  private final ScheduledExecutorService service =
      ThreadUtils.getDaemonSingleThreadScheduledExecutor("startHeartBeat");
  private final ExecutorService heartBeatExecutorService;

  public RegisterHeartBeat(ShuffleServer shuffleServer) {
    ShuffleServerConf conf = shuffleServer.getShuffleServerConf();
    this.heartBeatInitialDelay = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_DELAY);
    this.heartBeatInterval = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL);
    this.coordinatorQuorum = conf.getString(ShuffleServerConf.RSS_COORDINATOR_QUORUM);
    CoordinatorClientFactory factory = CoordinatorClientFactory.getInstance();
    this.coordinatorClients =
        factory.createCoordinatorClient(
            conf.get(ShuffleServerConf.RSS_COORDINATOR_CLIENT_TYPE), this.coordinatorQuorum);
    this.shuffleServer = shuffleServer;
    this.heartBeatExecutorService =
        ThreadUtils.getDaemonFixedThreadPool(
            conf.getInteger(ShuffleServerConf.SERVER_HEARTBEAT_THREAD_NUM), "sendHeartBeat");
  }

  public void startHeartBeat() {
    LOG.info(
        "Start heartbeat to coordinator {} after {}ms and interval is {}ms",
        coordinatorQuorum,
        heartBeatInitialDelay,
        heartBeatInterval);
    Runnable runnable =
        () -> {
          try {
            sendHeartBeat(
                shuffleServer.getId(),
                shuffleServer.getIp(),
                shuffleServer.getGrpcPort(),
                shuffleServer.getUsedMemory(),
                shuffleServer.getPreAllocatedMemory(),
                shuffleServer.getAvailableMemory(),
                shuffleServer.getEventNumInFlush(),
                shuffleServer.getTags(),
                shuffleServer.getServerStatus(),
                shuffleServer.getStorageManager().getStorageInfo(),
                shuffleServer.getNettyPort(),
                shuffleServer.getJettyPort(),
                shuffleServer.getStartTimeMs(),
                shuffleServer.getAppInfos());
          } catch (Exception e) {
            LOG.warn("Error happened when send heart beat to coordinator");
          }
        };
    service.scheduleAtFixedRate(
        runnable, heartBeatInitialDelay, heartBeatInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public boolean sendHeartBeat(
      String id,
      String ip,
      int grpcPort,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus serverStatus,
      Map<String, StorageInfo> localStorageInfo,
      int nettyPort,
      int jettyPort,
      long startTimeMs,
      List<RssProtos.ApplicationInfo> appInfos) {
    AtomicBoolean sendSuccessfully = new AtomicBoolean(false);
    // use `rss.server.heartbeat.interval` as the timeout option
    RssSendHeartBeatRequest request =
        new RssSendHeartBeatRequest(
            id,
            ip,
            grpcPort,
            usedMemory,
            preAllocatedMemory,
            availableMemory,
            eventNumInFlush,
            heartBeatInterval,
            tags,
            serverStatus,
            localStorageInfo,
            nettyPort,
            jettyPort,
            startTimeMs,
            appInfos);

    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        client -> client.sendHeartBeat(request),
        request.getTimeout() * 2,
        "send heartbeat",
        future -> {
          try {
            if (future.get(request.getTimeout() * 2, TimeUnit.MILLISECONDS).getStatusCode()
                == StatusCode.SUCCESS) {
              sendSuccessfully.set(true);
            }
          } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
          }
          return null;
        });

    return sendSuccessfully.get();
  }

  public void shutdown() {
    heartBeatExecutorService.shutdownNow();
    service.shutdownNow();
  }
}
