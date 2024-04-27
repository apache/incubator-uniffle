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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.proto.RssProtos;

public class RssReportShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private long taskAttemptId;
  private int bitmapNum;
  private Map<Integer, List<BlockId>> partitionToBlockIds;

  public RssReportShuffleResultRequest(
      String appId,
      int shuffleId,
      long taskAttemptId,
      Map<Integer, List<BlockId>> partitionToBlockIds,
      int bitmapNum) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskAttemptId = taskAttemptId;
    this.bitmapNum = bitmapNum;
    this.partitionToBlockIds = partitionToBlockIds;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getBitmapNum() {
    return bitmapNum;
  }

  public Map<Integer, List<BlockId>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }

  public RssProtos.ReportShuffleResultRequest toProto() {
    RssReportShuffleResultRequest request = this;
    List<RssProtos.PartitionToBlockIds> partitionToBlockIds = Lists.newArrayList();
    for (Map.Entry<Integer, List<BlockId>> entry : request.getPartitionToBlockIds().entrySet()) {
      List<BlockId> blockIds = entry.getValue();
      if (blockIds != null && !blockIds.isEmpty()) {
        partitionToBlockIds.add(
            RssProtos.PartitionToBlockIds.newBuilder()
                .setPartitionId(entry.getKey())
                .addAllBlockIds(blockIds.stream().map(BlockId::getBlockId).collect(Collectors.toList()))
                .build());
      }
    }

    RssProtos.ReportShuffleResultRequest rpcRequest =
        RssProtos.ReportShuffleResultRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setTaskAttemptId(request.getTaskAttemptId())
            .setBitmapNum(request.getBitmapNum())
            .addAllPartitionToBlockIds(partitionToBlockIds)
            .build();
    return rpcRequest;
  }
}
