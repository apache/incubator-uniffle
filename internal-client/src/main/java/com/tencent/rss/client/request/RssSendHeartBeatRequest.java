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

import java.util.Set;

public class RssSendHeartBeatRequest {

  private final String shuffleServerId;
  private final String shuffleServerIp;
  private final int shuffleServerPort;
  private final long usedMemory;
  private final long preAllocatedMemory;
  private final long availableMemory;
  private final int eventNumInFlush;
  private final Set<String> tags;
  private final long timeout;
  private final boolean isHealthy;

  public RssSendHeartBeatRequest(
      String shuffleServerId,
      String shuffleServerIp,
      int shuffleServerPort,
      long usedMemory,
      long preAllocatedMemory,
      long availableMemory,
      int eventNumInFlush,
      long timeout,
      Set<String> tags,
      boolean isHealthy) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServerIp = shuffleServerIp;
    this.shuffleServerPort = shuffleServerPort;
    this.usedMemory = usedMemory;
    this.preAllocatedMemory = preAllocatedMemory;
    this.availableMemory = availableMemory;
    this.eventNumInFlush = eventNumInFlush;
    this.tags = tags;
    this.timeout = timeout;
    this.isHealthy = isHealthy;
  }

  public String getShuffleServerId() {
    return shuffleServerId;
  }

  public String getShuffleServerIp() {
    return shuffleServerIp;
  }

  public int getShuffleServerPort() {
    return shuffleServerPort;
  }

  public long getTimeout() {
    return timeout;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public long getPreAllocatedMemory() {
    return preAllocatedMemory;
  }

  public long getAvailableMemory() {
    return availableMemory;
  }

  public int getEventNumInFlush() {
    return eventNumInFlush;
  }

  public Set<String> getTags() {
    return tags;
  }

  public boolean isHealthy() {
    return isHealthy;
  }
}
