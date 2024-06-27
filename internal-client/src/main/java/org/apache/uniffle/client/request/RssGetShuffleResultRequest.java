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

import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.proto.RssProtos;

public class RssGetShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private BlockIdLayout layout;

  public RssGetShuffleResultRequest(
      String appId, int shuffleId, int partitionId, BlockIdLayout layout) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.layout = layout;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public BlockIdLayout getBlockIdLayout() {
    return layout;
  }

  public RssProtos.GetShuffleResultRequest toProto() {
    RssGetShuffleResultRequest request = this;
    RssProtos.GetShuffleResultRequest rpcRequest =
        RssProtos.GetShuffleResultRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setBlockIdLayout(
                RssProtos.BlockIdLayout.newBuilder()
                    .setSequenceNoBits(request.getBlockIdLayout().sequenceNoBits)
                    .setPartitionIdBits(request.getBlockIdLayout().partitionIdBits)
                    .setTaskAttemptIdBits(request.getBlockIdLayout().taskAttemptIdBits)
                    .build())
            .build();
    return rpcRequest;
  }
}
