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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_ASSGINMENT_HOST_STRATEGY;

public abstract class AbstractAssignmentStrategy implements AssignmentStrategy {
  protected final CoordinatorConf conf;
  private final HostAssignmentStrategy assignmentHostStrategy;

  public AbstractAssignmentStrategy(CoordinatorConf conf) {
    this.conf = conf;
    assignmentHostStrategy = conf.get(COORDINATOR_ASSGINMENT_HOST_STRATEGY);
  }

  protected List<ServerNode> getCandidateNodes(List<ServerNode> allNodes, int expectNum) {
    switch (assignmentHostStrategy) {
      case MUST_DIFF: return getCandidateNodesWithDiffHost(allNodes, expectNum);
      case PREFER_DIFF: return tryGetCandidateNodesWithDiffHost(allNodes, expectNum);
      case NONE: return allNodes.subList(0, expectNum);
      default: throw new RuntimeException("Unsupported host assignment strategy:" + assignmentHostStrategy);
    }
  }

  protected List<ServerNode> tryGetCandidateNodesWithDiffHost(List<ServerNode> allNodes, int expectNum) {
    List<ServerNode> candidatesNodes = getCandidateNodesWithDiffHost(allNodes, expectNum);
    Set<ServerNode> candidatesNodeSet = candidatesNodes.stream().collect(Collectors.toSet());
    if (candidatesNodes.size() < expectNum) {
      for (ServerNode node : allNodes) {
        if (candidatesNodeSet.contains(node)) {
          continue;
        }
        candidatesNodes.add(node);
        if (candidatesNodes.size() >= expectNum) {
          break;
        }
      }
    }
    return candidatesNodes;
  }

  protected List<ServerNode> getCandidateNodesWithDiffHost(List<ServerNode> allNodes, int expectNum) {
    List<ServerNode> candidatesNodes = new ArrayList<>();
    Set<String> hostIpCandidate = new HashSet<>();
    for (ServerNode node : allNodes) {
      if (hostIpCandidate.contains(node.getIp())) {
        continue;
      }
      hostIpCandidate.add(node.getIp());
      candidatesNodes.add(node);
      if (candidatesNodes.size() >= expectNum) {
        break;
      }
    }
    return candidatesNodes;
  }


  public enum HostAssignmentStrategy {
    MUST_DIFF,
    PREFER_DIFF,
    NONE
  }
}
