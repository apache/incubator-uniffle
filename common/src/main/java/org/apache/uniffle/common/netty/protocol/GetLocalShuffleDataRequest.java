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

package org.apache.uniffle.common.netty.protocol;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.util.ByteBufUtils;

public class GetLocalShuffleDataRequest extends RequestMessage {
  private String appId;
  private int shuffleId;
  private int partitionId;
  private int partitionNumPerRange;
  private int partitionNum;
  private long offset;
  private int length;
  private long timestamp;

  public GetLocalShuffleDataRequest(
      long requestId,
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      long offset,
      int length,
      long timestamp) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.offset = offset;
    this.length = length;
    this.timestamp = timestamp;
  }

  @Override
  public Type type() {
    return Type.GET_LOCAL_SHUFFLE_DATA_REQUEST;
  }

  @Override
  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH
        + ByteBufUtils.encodedLength(appId)
        + 2 * Long.BYTES
        + 5 * Integer.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeInt(partitionNumPerRange);
    buf.writeInt(partitionNum);
    buf.writeLong(offset);
    buf.writeInt(length);
    buf.writeLong(timestamp);
  }

  public static GetLocalShuffleDataRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    int partitionId = byteBuf.readInt();
    int partitionNumPerRange = byteBuf.readInt();
    int partitionNum = byteBuf.readInt();
    long offset = byteBuf.readLong();
    int length = byteBuf.readInt();
    long timestamp = byteBuf.readLong();
    return new GetLocalShuffleDataRequest(
        requestId,
        appId,
        shuffleId,
        partitionId,
        partitionNumPerRange,
        partitionNum,
        offset,
        length,
        timestamp);
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

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
