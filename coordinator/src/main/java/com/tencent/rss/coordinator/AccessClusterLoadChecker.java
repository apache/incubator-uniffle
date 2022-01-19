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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccessClusterLoadChecker use the cluster load metrics including memory and healthy to
 * filter and count available nodes numbers and reject if the number do not reach the threshold.
 */
public class AccessClusterLoadChecker implements AccessChecker {

  private static final Logger LOG = LoggerFactory.getLogger(AccessClusterLoadChecker.class);

  private final ClusterManager clusterManager;
  private final double memoryPercentThreshold;
  private final int availableServerNumThreshold;

  public AccessClusterLoadChecker(AccessManager accessManager) {
    clusterManager = accessManager.getClusterManager();
    CoordinatorConf conf = accessManager.getCoordinatorConf();
    this.memoryPercentThreshold = conf.getDouble(CoordinatorConf.COORDINATOR_ACCESS_LOADCHECKER_MEMORY_PERCENTAGE);
    this.availableServerNumThreshold = conf.getInteger(
        CoordinatorConf.COORDINATOR_ACCESS_LOADCHECKER_SERVER_NUM_THRESHOLD,
        conf.get(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX));
  }

  public AccessCheckResult check(AccessInfo accessInfo) {
    Set<String> tags = accessInfo.getTags();
    List<ServerNode> servers = clusterManager.getServerList(tags);
    int size = (int) servers.stream().filter(ServerNode::isHealthy).filter(this::checkMemory).count();
    if (size >= availableServerNumThreshold) {
      return new AccessCheckResult(true, "SUCCESS");
    } else {
      String msg = String.format("%s cluster load check failed total %s nodes, %s available nodes, "
              + "memory percent threshold %s, available num threshold %s.",
          accessInfo, servers.size(), size, memoryPercentThreshold, availableServerNumThreshold);
      LOG.warn(msg);
      return new AccessCheckResult(false, msg);
    }
  }

  private boolean checkMemory(ServerNode serverNode) {
    double availableMemory = (double) serverNode.getAvailableMemory();
    double total = (double) serverNode.getTotalMemory();
    double availablePercent = availableMemory / (total / 100.0);
    return Double.compare(availablePercent, memoryPercentThreshold) >= 0;
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public double getMemoryPercentThreshold() {
    return memoryPercentThreshold;
  }

  public int getAvailableServerNumThreshold() {
    return availableServerNumThreshold;
  }

  public void close() {
  }
}
