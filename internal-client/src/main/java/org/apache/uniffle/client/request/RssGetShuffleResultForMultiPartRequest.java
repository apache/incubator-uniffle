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

package org.apache.uniffle.client.request;

import java.util.Set;

import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.proto.RssProtos;

public class RssGetShuffleResultForMultiPartRequest {
  private String appId;
  private int shuffleId;
  private Set<Integer> partitions;
  private BlockIdLayout blockIdLayout;

  private int stageAttemptNumber;

  public RssGetShuffleResultForMultiPartRequest(
      String appId, int shuffleId, Set<Integer> partitions, BlockIdLayout blockIdLayout, int stageAttemptNumbers) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitions = partitions;
    this.blockIdLayout = blockIdLayout;
    this.stageAttemptNumber = stageAttemptNumbers;
  }

  public RssGetShuffleResultForMultiPartRequest(
      String appId, int shuffleId, Set<Integer> partitions, BlockIdLayout blockIdLayout) {
    this(appId, shuffleId, partitions, blockIdLayout, 0);
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public Set<Integer> getPartitions() {
    return partitions;
  }

  public BlockIdLayout getBlockIdLayout() {
    return blockIdLayout;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  public RssProtos.GetShuffleResultForMultiPartRequest toProto() {
    RssGetShuffleResultForMultiPartRequest request = this;
    RssProtos.GetShuffleResultForMultiPartRequest rpcRequest =
        RssProtos.GetShuffleResultForMultiPartRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .addAllPartitions(request.getPartitions())
            .setBlockIdLayout(
                RssProtos.BlockIdLayout.newBuilder()
                    .setSequenceNoBits(request.getBlockIdLayout().sequenceNoBits)
                    .setPartitionIdBits(request.getBlockIdLayout().partitionIdBits)
                    .setTaskAttemptIdBits(request.getBlockIdLayout().taskAttemptIdBits)
                    .build())
            .setStageAttemptNumber(request.getStageAttemptNumber())
            .build();
    return rpcRequest;
  }
}
