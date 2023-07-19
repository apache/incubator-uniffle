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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;

public class ServerNode implements Comparable<ServerNode> {

  private String id;
  private String ip;
  private int grpcPort;
  private long usedMemory;
  private long preAllocatedMemory;
  private long availableMemory;
  private int eventNumInFlush;
  private long timestamp;
  private Set<String> tags;
  private ServerStatus status;
  private Map<String, StorageInfo> storageInfo;
  private int nettyPort = -1;

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
    this.id = id;
    this.ip = ip;
    this.grpcPort = grpcPort;
    this.usedMemory = usedMemory;
    this.preAllocatedMemory = preAllocatedMemory;
    this.availableMemory = availableMemory;
    this.eventNumInFlush = eventNumInFlush;
    this.timestamp = System.currentTimeMillis();
    this.tags = tags;
    this.status = status;
    this.storageInfo = storageInfoMap;
    if (nettyPort > 0) {
      this.nettyPort = nettyPort;
    }
  }

  public ShuffleServerId convertToGrpcProto() {
    return ShuffleServerId.newBuilder()
        .setId(id)
        .setIp(ip)
        .setPort(grpcPort)
        .setNettyPort(nettyPort)
        .build();
  }

  public String getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getPreAllocatedMemory() {
    return preAllocatedMemory;
  }

  public long getAvailableMemory() {
    return availableMemory;
  }

  public int getEventNumInFlush() {
    return eventNumInFlush;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public Set<String> getTags() {
    return tags;
  }

  public ServerStatus getStatus() {
    return status;
  }

  public void setStatus(ServerStatus serverStatus) {
    this.status = serverStatus;
  }

  public Map<String, StorageInfo> getStorageInfo() {
    return storageInfo;
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
        + "], tags"
        + tags.toString()
        + ""
        + ", status["
        + status
        + "], storages[num="
        + storageInfo.size()
        + "]";
  }

  /** Only for test case */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int compareTo(ServerNode other) {
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
    if (obj instanceof ServerNode) {
      return id.equals(((ServerNode) obj).getId());
    }
    return false;
  }

  public long getTotalMemory() {
    return availableMemory + usedMemory;
  }

  public int getNettyPort() {
    return nettyPort;
  }
}
