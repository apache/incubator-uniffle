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

package org.apache.spark.shuffle.writer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.shuffle.ShuffleHandleInfo;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;

/** This class is to wrap the shuffleHandleInfo to speed up the partitionAssignment getting. */
public class ShuffleHandleInfoWrapper {
  private ShuffleHandleInfo handle;
  private final long taskAttemptId;
  private final Set<String> faultyServers;
  private Map<Integer, List<ShuffleServerInfo>> latestAssignment;

  public ShuffleHandleInfoWrapper(long taskAttemptId, ShuffleHandleInfo shuffleHandleInfo) {
    this.taskAttemptId = taskAttemptId;
    this.faultyServers = new HashSet<>();
    this.update(shuffleHandleInfo, null);
  }

  public List<ShuffleServerInfo> retrievePartitionAssignment(int partitiontId) {
    return latestAssignment.get(partitiontId);
  }

  public boolean isReassigned(String serverId) {
    return faultyServers.contains(serverId);
  }

  public void update(ShuffleHandleInfo handle, String faultyServerId) {
    if (handle == null) {
      throw new RssException("Errors on updating shuffle handle by the empty handleInfo.");
    }
    this.handle = handle;
    this.latestAssignment = handle.getLatestAssignmentPlan(taskAttemptId);
    if (faultyServerId != null) {
      this.faultyServers.add(faultyServerId);
    }
  }

  // Only for tests
  @VisibleForTesting
  public ShuffleHandleInfo getRef() {
    return handle;
  }
}
