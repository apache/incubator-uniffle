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

package org.apache.uniffle.server.netty.message;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class GetInMemoryShuffleDataMessage {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private int readBufferSize;
  private long lastBlockId;

  public void readMessageInfo(ByteBuf buf, int appIdLength) {
    byte[] bytes = new byte[appIdLength];
    buf.readBytes(bytes);
    appId = new String(bytes, StandardCharsets.UTF_8);
    shuffleId = buf.readInt();
    partitionId = buf.readInt();
    readBufferSize = buf.readInt();
    lastBlockId = buf.readLong();
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

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public long getLastBlockId() {
    return lastBlockId;
  }
}
