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

package org.apache.uniffle.coordinator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.coordinator.web.vo.ServerNodeVO;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;

public class ServerNode implements Comparable<ServerNode> {
  private final ServerNodeVO serverNodeVO;
  private final Map<String, RssProtos.ApplicationInfo> appIdToInfos;

  public Map<String, RssProtos.ApplicationInfo> getAppIdToInfos() {
    return appIdToInfos;
  }

  public void combineAppInfos(ServerNode oldServerNode) {
    if (oldServerNode == null) {
      return;
    }
    for (Map.Entry<String, RssProtos.ApplicationInfo> entry :
        oldServerNode.getAppIdToInfos().entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      if (!appIdToInfos.containsKey(entry.getKey())) {
        appIdToInfos.put(entry.getKey(), entry.getValue());
      } else {
        RssProtos.ApplicationInfo current = appIdToInfos.get(entry.getKey());
        RssProtos.ApplicationInfo other = entry.getValue();
        // keep the max info
        if (current.getTotalSize() > other.getTotalSize()) {
          appIdToInfos.put(entry.getKey(), current);
        } else {
          appIdToInfos.put(entry.getKey(), other);
        }
      }
    }
  }

  public ServerNode(String id) {
    this(id, "", 0, 0, 0, 0, 0, Sets.newHashSet(), ServerStatus.EXCLUDED);
  }

  // Only for test
  public ServerNode(
      String id,
      String ip,
      int port,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags) {
    this(
        id,
        ip,
        port,
        usedMemory,
        preAllocatedMemory,
        availableMemory,
        eventNumInFlush,
        tags,
        ServerStatus.ACTIVE,
        Maps.newHashMap());
  }

  public ServerNode(
      String id,
      String ip,
      int port,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus status) {
    this(
        id,
        ip,
        port,
        usedMemory,
        preAllocatedMemory,
        availableMemory,
        eventNumInFlush,
        tags,
        status,
        Maps.newHashMap());
  }

  public ServerNode(
      String id,
      String ip,
      int port,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus status,
      Map<String, StorageInfo> storageInfoMap) {
    this(
        id,
        ip,
        port,
        usedMemory,
        preAllocatedMemory,
        availableMemory,
        eventNumInFlush,
        tags,
        status,
        storageInfoMap,
        -1,
        -1,
        -1);
  }

  public ServerNode(
      String id,
      String ip,
      int grpcPort,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus status,
      Map<String, StorageInfo> storageInfoMap,
      int nettyPort) {
    this(
        id,
        ip,
        grpcPort,
        usedMemory,
        preAllocatedMemory,
        availableMemory,
        eventNumInFlush,
        tags,
        status,
        storageInfoMap,
        nettyPort,
        -1,
        -1L);
  }

  public ServerNode(
      String id,
      String ip,
      int grpcPort,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus status,
      Map<String, StorageInfo> storageInfoMap,
      int nettyPort,
      int jettyPort,
      long startTime) {
    this(
        id,
        ip,
        grpcPort,
        usedMemory,
        preAllocatedMemory,
        availableMemory,
        eventNumInFlush,
        tags,
        status,
        storageInfoMap,
        nettyPort,
        jettyPort,
        startTime,
        "",
        "",
        Collections.emptyList());
  }

  public ServerNode(
      String id,
      String ip,
      int grpcPort,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      Set<String> tags,
      ServerStatus status,
      Map<String, StorageInfo> storageInfoMap,
      int nettyPort,
      int jettyPort,
      long startTime,
      String version,
      String gitCommitId,
      List<RssProtos.ApplicationInfo> appInfos) {
    this.serverNodeVO =
        new ServerNodeVO(
            id,
            ip,
            grpcPort,
            usedMemory,
            preAllocatedMemory,
            availableMemory,
            eventNumInFlush,
            tags,
            status,
            storageInfoMap,
            nettyPort,
            jettyPort,
            startTime,
            version,
            gitCommitId);
    this.appIdToInfos = new ConcurrentHashMap<>();
    for (RssProtos.ApplicationInfo app : appInfos) {
      this.appIdToInfos.put(app.getAppId(), app);
    }
  }

  public ShuffleServerId convertToGrpcProto() {
    return ShuffleServerId.newBuilder()
        .setId(serverNodeVO.getId())
        .setIp(serverNodeVO.getIp())
        .setPort(serverNodeVO.getGrpcPort())
        .setNettyPort(serverNodeVO.getNettyPort())
        .setJettyPort(serverNodeVO.getJettyPort())
        .build();
  }

  public String getId() {
    return serverNodeVO.getId();
  }

  public String getIp() {
    return serverNodeVO.getIp();
  }

  public int getGrpcPort() {
    return serverNodeVO.getGrpcPort();
  }

  public long getTimestamp() {
    return serverNodeVO.getTimestamp();
  }

  public long getPreAllocatedMemory() {
    return serverNodeVO.getPreAllocatedMemory();
  }

  public long getAvailableMemory() {
    return serverNodeVO.getAvailableMemory();
  }

  public int getEventNumInFlush() {
    return serverNodeVO.getEventNumInFlush();
  }

  public long getUsedMemory() {
    return serverNodeVO.getUsedMemory();
  }

  public Set<String> getTags() {
    return serverNodeVO.getTags();
  }

  public ServerStatus getStatus() {
    return serverNodeVO.getStatus();
  }

  public void setStatus(ServerStatus serverStatus) {
    serverNodeVO.setStatus(serverStatus);
  }

  public Map<String, StorageInfo> getStorageInfo() {
    return serverNodeVO.getStorageInfo();
  }

  @Override
  public String toString() {
    return "ServerNode with id["
        + serverNodeVO.getId()
        + "], ip["
        + serverNodeVO.getIp()
        + "], grpc port["
        + serverNodeVO.getGrpcPort()
        + "], netty port["
        + serverNodeVO.getNettyPort()
        + "], jettyPort["
        + serverNodeVO.getJettyPort()
        + "], usedMemory["
        + serverNodeVO.getUsedMemory()
        + "], preAllocatedMemory["
        + serverNodeVO.getPreAllocatedMemory()
        + "], availableMemory["
        + serverNodeVO.getAvailableMemory()
        + "], eventNumInFlush["
        + serverNodeVO.getEventNumInFlush()
        + "], timestamp["
        + serverNodeVO.getTimestamp()
        + "], tags["
        + serverNodeVO.getTags().toString()
        + "], status["
        + serverNodeVO.getStatus()
        + "], storages[num="
        + serverNodeVO.getStorageInfo().size()
        + "], version["
        + serverNodeVO.getVersion()
        + "], gitCommitId["
        + serverNodeVO.getGitCommitId()
        + "]";
  }

  /** Only for test case */
  public void setTimestamp(long timestamp) {
    serverNodeVO.setTimestamp(timestamp);
  }

  void setRegistrationTime(long registrationTime) {
    serverNodeVO.setRegistrationTime(registrationTime);
  }

  public long getRegistrationTime() {
    return serverNodeVO.getRegistrationTime();
  }

  @Override
  public int compareTo(ServerNode other) {
    if (getAvailableMemory() > other.getAvailableMemory()) {
      return -1;
    } else if (getAvailableMemory() < other.getAvailableMemory()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return serverNodeVO.getId().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ServerNode) {
      return serverNodeVO.getId().equals(((ServerNode) obj).getId());
    }
    return false;
  }

  public long getTotalMemory() {
    return serverNodeVO.getAvailableMemory() + serverNodeVO.getUsedMemory();
  }

  public int getNettyPort() {
    return serverNodeVO.getNettyPort();
  }

  public int getJettyPort() {
    return serverNodeVO.getJettyPort();
  }

  public long getStartTime() {
    return serverNodeVO.getStartTime();
  }

  public String getVersion() {
    return serverNodeVO.getVersion();
  }

  public String getGitCommitId() {
    return serverNodeVO.getGitCommitId();
  }

  public ServerNodeVO getServerNodeVO() {
    return serverNodeVO;
  }
}
