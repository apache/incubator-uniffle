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

package org.apache.spark.shuffle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.JavaUtils;

/**
 * Class for holding, 1. partition ID -> shuffle servers mapping. 2. remote storage info
 *
 * <p>It's to be broadcast to executors and referenced by shuffle tasks.
 */
public class ShuffleHandleInfo implements Serializable {

  private int shuffleId;

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  // partitionId -> replica -> failover servers
  private Map<Integer, Map<Integer, ShuffleServerInfo>> failoverPartitionServers;
  // todo: support mores replacement servers for one faulty server.
  private Map<String, ShuffleServerInfo> faultyServerReplacements;

  // shuffle servers which is for store shuffle data
  private Set<ShuffleServerInfo> shuffleServersForData;
  // remoteStorage used for this job
  private RemoteStorageInfo remoteStorage;

  public static final ShuffleHandleInfo EMPTY_HANDLE_INFO =
      new ShuffleHandleInfo(-1, Collections.EMPTY_MAP, RemoteStorageInfo.EMPTY_REMOTE_STORAGE);

  public ShuffleHandleInfo(
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo) {
    this.shuffleId = shuffleId;
    this.partitionToServers = partitionToServers;
    this.shuffleServersForData = Sets.newHashSet();
    this.failoverPartitionServers = Maps.newConcurrentMap();
    for (List<ShuffleServerInfo> ssis : partitionToServers.values()) {
      this.shuffleServersForData.addAll(ssis);
    }
    this.remoteStorage = storageInfo;
    this.faultyServerReplacements = JavaUtils.newConcurrentMap();
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return shuffleServersForData;
  }

  public RemoteStorageInfo getRemoteStorage() {
    return remoteStorage;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public boolean isExistingFaultyServer(String serverId) {
    return faultyServerReplacements.containsKey(serverId);
  }

  public ShuffleServerInfo useExistingReassignmentForMultiPartitions(
      Set<Integer> partitionIds, String faultyServerId) {
    return createNewReassignmentForMultiPartitions(partitionIds, faultyServerId, null);
  }

  public ShuffleServerInfo createNewReassignmentForMultiPartitions(
      Set<Integer> partitionIds, String faultyServerId, ShuffleServerInfo replacement) {
    if (replacement != null) {
      faultyServerReplacements.put(faultyServerId, replacement);
    }

    replacement = faultyServerReplacements.get(faultyServerId);
    for (Integer partitionId : partitionIds) {
      List<ShuffleServerInfo> replicaServers = partitionToServers.get(partitionId);
      for (int i = 0; i < replicaServers.size(); i++) {
        if (replicaServers.get(i).getId().equals(faultyServerId)) {
          Map<Integer, ShuffleServerInfo> replicaReplacements =
              failoverPartitionServers.computeIfAbsent(
                  partitionId, k -> JavaUtils.newConcurrentMap());
          replicaReplacements.put(i, replacement);
        }
      }
    }
    return replacement;
  }

  /** This composes the partition's replica server + replacement servers. */
  public Map<Integer, List<ShuffleServerInfo>> listAllPartitionAssignmentServers() {
    Map<Integer, List<ShuffleServerInfo>> partitionServer = new HashMap<>();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      int partitionId = entry.getKey();
      List<ShuffleServerInfo> replicas = entry.getValue();
      Map<Integer, ShuffleServerInfo> replacements = failoverPartitionServers.get(partitionId);
      if (replacements == null) {
        replacements = Collections.emptyMap();
      }

      List<ShuffleServerInfo> servers =
          partitionServer.computeIfAbsent(partitionId, k -> new ArrayList<>());
      for (int i = 0; i < replicas.size(); i++) {
        servers.add(replicas.get(i));
        ShuffleServerInfo replacement = replacements.get(i);
        if (replacement != null) {
          servers.add(replacement);
        }
      }
    }
    return partitionServer;
  }
}
