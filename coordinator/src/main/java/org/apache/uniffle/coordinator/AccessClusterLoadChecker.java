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

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.Constants;

/**
 * AccessClusterLoadChecker use the cluster load metrics including memory and healthy to
 * filter and count available nodes numbers and reject if the number do not reach the threshold.
 */
public class AccessClusterLoadChecker extends AccessChecker {

  private static final Logger LOG = LoggerFactory.getLogger(AccessClusterLoadChecker.class);

  private final ClusterManager clusterManager;
  private final double memoryPercentThreshold;
  private final int availableServerNumThreshold;

  public AccessClusterLoadChecker(AccessManager accessManager) throws Exception {
    super(accessManager);
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
      return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE);
    } else {
      String msg = String.format("Denied by AccessClusterLoadChecker accessInfo[%s], "
              + "total %s nodes, %s available nodes, "
              + "memory percent threshold %s, available num threshold %s.",
          accessInfo, servers.size(), size, memoryPercentThreshold, availableServerNumThreshold);
      LOG.warn(msg);
      CoordinatorMetrics.counterTotalLoadDeniedRequest.inc();
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
