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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;

public class UploadDataMessage {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataMessage.class);

  private long requestId;
  private String appId;
  private int shuffleId;
  private long requireBufferId;
  private Map<Integer, List<ShufflePartitionedBlock>> shuffleData = Maps.newHashMap();

  public void readMessageInfo(ByteBuf buf, int appIdLength) {
    byte[] bytes = new byte[appIdLength];
    buf.readBytes(bytes);
    appId = new String(bytes, StandardCharsets.UTF_8);
    shuffleId = buf.readInt();
    requireBufferId = buf.readLong();
  }

  public void addBlockData(
      int partitionId,
      long blockId,
      long crc,
      int uncompressLength,
      int dataLength,
      long taskAttemptId,
      ByteBuf buf) {
    if (!shuffleData.containsKey(partitionId)) {
      shuffleData.put(partitionId, Lists.newArrayList());
    }
    List<ShufflePartitionedBlock> dataList = shuffleData.get(partitionId);
    dataList.add(new ShufflePartitionedBlock(dataLength, uncompressLength, crc, blockId, taskAttemptId,
        buf.array()));
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  public long getRequestId() {
    return requestId;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getRequireBufferId() {
    return requireBufferId;
  }

  public Map<Integer, List<ShufflePartitionedBlock>> getShuffleData() {
    return shuffleData;
  }
}
