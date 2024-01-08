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

package org.apache.uniffle.common;

import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PartitionServerInfo {
  private Integer partitionId;
  private List<ShuffleServerInfo> splitServers;

  public Integer getPartitionId() {
    return partitionId;
  }

  public PartitionServerInfo(Integer partitionId, List<ShuffleServerInfo> splitServers) {
    this.partitionId = partitionId;
    this.splitServers = splitServers;
  }

  public List<ShuffleServerInfo> getSplitServers() {
    return splitServers;
  }

  public ShuffleServerInfo getFirstSplitServer() {
    return splitServers != null && splitServers.size() > 0 ? splitServers.get(0) : null;
  }

  public ShuffleServerInfo getLastSplitServer() {
    return splitServers != null && splitServers.size() > 0
        ? splitServers.get(splitServers.size() - 1)
        : null;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder codeBuilder = new HashCodeBuilder();
    codeBuilder.append(this.getPartitionId());
    if (this.getSplitServers() != null) {
      for (int i = 0; i < this.getSplitServers().size(); i++) {
        codeBuilder.append(this.getSplitServers().get(i));
      }
    }
    return codeBuilder.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartitionServerInfo) {
      return partitionId.equals(((PartitionServerInfo) obj).getPartitionId())
          && splitServers.equals(((PartitionServerInfo) obj).getSplitServers());
    }
    return false;
  }

}
