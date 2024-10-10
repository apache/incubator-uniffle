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

package org.apache.uniffle.coordinator.web.vo;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.storage.StorageInfo;

@Data
@Getter
@Setter
public class ServerNodeVO implements Comparable<ServerNodeVO> {

  private String id;
  private String ip;
  private int grpcPort;
  private long usedMemory;
  private long preAllocatedMemory;
  private long availableMemory;
  private int eventNumInFlush;
  private long registrationTime;
  private long timestamp;
  private Set<String> tags;
  private ServerStatus status;
  private Map<String, StorageInfo> storageInfo;
  private int nettyPort = -1;
  private int jettyPort = -1;
  private long startTime = -1;
  private String version;
  private String gitCommitId;

  public ServerNodeVO(String id) {
    this(id, "", 0, 0, 0, 0, 0, Sets.newHashSet(), ServerStatus.EXCLUDED);
  }

  // Only for test
  public ServerNodeVO(
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

  public ServerNodeVO(
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

  public ServerNodeVO(
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

  public ServerNodeVO(
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

  public ServerNodeVO(
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
        -1,
        -1L,
        "",
        "");
  }

  public ServerNodeVO(
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
      String gitCommitId) {
    this.id = id;
    this.ip = ip;
    this.grpcPort = grpcPort;
    this.usedMemory = usedMemory;
    this.preAllocatedMemory = preAllocatedMemory;
    this.availableMemory = availableMemory;
    this.eventNumInFlush = eventNumInFlush;
    this.registrationTime = System.currentTimeMillis();
    this.timestamp = registrationTime;
    this.tags = tags;
    this.status = status;
    this.storageInfo = storageInfoMap;
    if (nettyPort > 0) {
      this.nettyPort = nettyPort;
    }
    if (jettyPort > 0) {
      this.jettyPort = jettyPort;
    }
    this.startTime = startTime;
    this.version = version;
    this.gitCommitId = gitCommitId;
  }

  @Override
  public String toString() {
    return "ServerNode with id["
        + id
        + "], ip["
        + ip
        + "], grpc port["
        + grpcPort
        + "], netty port["
        + nettyPort
        + "], jettyPort["
        + jettyPort
        + "], usedMemory["
        + usedMemory
        + "], preAllocatedMemory["
        + preAllocatedMemory
        + "], availableMemory["
        + availableMemory
        + "], eventNumInFlush["
        + eventNumInFlush
        + "], timestamp["
        + timestamp
        + "], tags["
        + tags.toString()
        + "], status["
        + status
        + "], storages[num="
        + storageInfo.size()
        + "], version["
        + version
        + "], gitCommitId["
        + gitCommitId
        + "]";
  }

  @Override
  public int compareTo(ServerNodeVO other) {
    if (availableMemory > other.getAvailableMemory()) {
      return -1;
    } else if (availableMemory < other.getAvailableMemory()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ServerNodeVO) {
      return id.equals(((ServerNodeVO) obj).getId());
    }
    return false;
  }
}
