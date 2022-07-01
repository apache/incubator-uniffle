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

package com.tencent.rss.client.request;

import java.util.List;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.RemoteStorageInfo;

public class RssRegisterShuffleRequest {

  private String appId;
  private int shuffleId;
  private List<PartitionRange> partitionRanges;
  private RemoteStorageInfo remoteStorageInfo;

  public RssRegisterShuffleRequest(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionRanges = partitionRanges;
    this.remoteStorageInfo = remoteStorageInfo;
  }

  public RssRegisterShuffleRequest(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      String remoteStoragePath) {
    this(appId, shuffleId, partitionRanges, new RemoteStorageInfo(remoteStoragePath));
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }

  public RemoteStorageInfo getRemoteStorageInfo() {
    return remoteStorageInfo;
  }
}
