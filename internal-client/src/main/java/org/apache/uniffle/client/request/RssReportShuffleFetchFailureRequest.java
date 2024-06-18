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

import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;

public class RssReportShuffleFetchFailureRequest {
  private String appId;
  private int shuffleId;
  private int stageAttemptId;
  private int partitionId;
  private String exception;
  private List<ShuffleServerInfo> fetchFailureServerInfos;
  private int stageId;
  private long taskAttemptId;
  private int taskAttemptNumber;
  private String executorId;

  public RssReportShuffleFetchFailureRequest(
      String appId,
      int shuffleId,
      int stageAttemptId,
      int partitionId,
      String exception,
      List<ShuffleServerInfo> fetchFailureServerInfos,
      int stageId,
      long taskAttemptId,
      int taskAttemptNumber,
      String executorId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.stageAttemptId = stageAttemptId;
    this.partitionId = partitionId;
    this.exception = exception;
    this.fetchFailureServerInfos = fetchFailureServerInfos;
    this.stageId = stageId;
    this.taskAttemptId = taskAttemptId;
    this.taskAttemptNumber = taskAttemptNumber;
    this.executorId = executorId;
  }

  // Only for tests
  @VisibleForTesting
  public RssReportShuffleFetchFailureRequest(
      String appId, int shuffleId, int stageAttemptId, int partitionId, String exception) {
    this(
        appId,
        shuffleId,
        stageAttemptId,
        partitionId,
        exception,
        Collections.emptyList(),
        0,
        0L,
        0,
        "executor1");
  }

  public ReportShuffleFetchFailureRequest toProto() {
    ReportShuffleFetchFailureRequest.Builder builder =
        ReportShuffleFetchFailureRequest.newBuilder();
    builder
        .setAppId(appId)
        .setShuffleId(shuffleId)
        .setStageAttemptId(stageAttemptId)
        .setPartitionId(partitionId)
        .setStageId(stageId)
        .setTaskAttemptId(taskAttemptId)
        .setTaskAttemptNumber(taskAttemptNumber)
        .setExecutorId(executorId)
        .addAllFetchFailureServerId(ShuffleServerInfo.toProto(fetchFailureServerInfos));
    if (exception != null) {
      builder.setException(exception);
    }
    return builder.build();
  }
}
