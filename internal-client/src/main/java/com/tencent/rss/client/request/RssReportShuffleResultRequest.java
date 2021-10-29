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

package com.tencent.rss.client.request;

import java.util.List;
import java.util.Map;

public class RssReportShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private long taskAttemptId;
  private int bitmapNum;
  private Map<Integer, List<Long>> partitionToBlockIds;

  public RssReportShuffleResultRequest(String appId, int shuffleId, long taskAttemptId,
      Map<Integer, List<Long>> partitionToBlockIds, int bitmapNum) {
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

  public Map<Integer, List<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
