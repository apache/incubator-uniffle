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

package org.apache.spark.shuffle.handle;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.ShuffleServerInfo;

public interface ShuffleHandleInfo {
  /** List all the assigned servers including the excluded servers. */
  Set<ShuffleServerInfo> listServers();

  /**
   * Get the latest assignment for writer to write partitioned blocks to corresponding
   * shuffleServers
   */
  Map<Integer, List<ShuffleServerInfo>> getPartitionToServers();

  /**
   * Get the all assigned servers group by partitionId for reader to get the data from these
   * historical and latest servers
   */
  Map<Integer, List<ShuffleServerInfo>> listPartitionServers();

  /** Create the partition replicas tracker for the writer to check data replica requirements */
  PartitionDataReplicaRequirementTracking createPartitionReplicaTracking();
}
