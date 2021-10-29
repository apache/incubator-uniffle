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

package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Set;

public interface ClusterManager {

  /**
   * Add a server to the cluster.
   *
   * @param shuffleServerInfo server info
   */
  void add(ServerNode shuffleServerInfo);

  /**
   * Get available nodes from the cluster
   *
   * @param requiredTags tags for filter
   * @return list of available server nodes
   */
  List<ServerNode> getServerList(Set<String> requiredTags);

  /**
   * @return number of server nodes in the cluster
   */
  int getNodesNum();

  /**
   * @return list all server nodes in the cluster
   */
  List<ServerNode> list();

  int getShuffleNodesMax();

  void shutdown();
}
