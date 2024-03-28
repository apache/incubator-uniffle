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

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.netty.DecodeException;
import org.apache.uniffle.common.netty.EncodeException;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.RssUtils;

public class GetMemoryShuffleDataRequest extends RequestMessage {
  private String appId;
  private int shuffleId;
  private int partitionId;
  private long lastBlockId;
  private int readBufferSize;
  private long timestamp;
  private Roaring64NavigableMap expectedTaskIdsBitmap;

  public GetMemoryShuffleDataRequest(
      long requestId,
      String appId,
      int shuffleId,
      int partitionId,
      long lastBlockId,
      int readBufferSize,
      long timestamp,
      Roaring64NavigableMap expectedTaskIdsBitmap) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.lastBlockId = lastBlockId;
    this.readBufferSize = readBufferSize;
    this.timestamp = timestamp;
    this.expectedTaskIdsBitmap = expectedTaskIdsBitmap;
  }

  @Override
  public Type type() {
    return Type.GET_MEMORY_SHUFFLE_DATA_REQUEST;
  }

  @Override
  public int encodedLength() {
    return (int)
        (REQUEST_ID_ENCODE_LENGTH
            + ByteBufUtils.encodedLength(appId)
            + 4 * Integer.BYTES
            + 2 * Long.BYTES
            + (expectedTaskIdsBitmap == null ? 0L : expectedTaskIdsBitmap.serializedSizeInBytes()));
  }

  @Override
  public void encode(ByteBuf buf) throws EncodeException {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeLong(lastBlockId);
    buf.writeInt(readBufferSize);
    buf.writeLong(timestamp);
    try {
      if (expectedTaskIdsBitmap != null) {
        buf.writeInt((int) expectedTaskIdsBitmap.serializedSizeInBytes());
        buf.writeBytes(RssUtils.serializeBitMap(expectedTaskIdsBitmap));
      } else {
        buf.writeInt(-1);
      }
    } catch (IOException ioException) {
      throw new EncodeException(
          "serializeBitMap failed while encode GetMemoryShuffleDataRequest!", ioException);
    }
  }

  public static GetMemoryShuffleDataRequest decode(ByteBuf byteBuf) throws DecodeException {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    int partitionId = byteBuf.readInt();
    long lastBlockId = byteBuf.readLong();
    int readBufferSize = byteBuf.readInt();
    long timestamp = byteBuf.readLong();
    byte[] bytes = ByteBufUtils.readByteArray(byteBuf);
    Roaring64NavigableMap expectedTaskIdsBitmap = null;
    try {
      if (bytes != null) {
        expectedTaskIdsBitmap = RssUtils.deserializeBitMap(bytes);
      }
    } catch (IOException ioException) {
      throw new DecodeException(
          "serializeBitMap failed while decode GetMemoryShuffleDataRequest!", ioException);
    }
    return new GetMemoryShuffleDataRequest(
        requestId,
        appId,
        shuffleId,
        partitionId,
        lastBlockId,
        readBufferSize,
        timestamp,
        expectedTaskIdsBitmap);
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

  public long getLastBlockId() {
    return lastBlockId;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Roaring64NavigableMap getExpectedTaskIdsBitmap() {
    return expectedTaskIdsBitmap;
  }

  @Override
  public String getOperationType() {
    return "getMemoryShuffleData";
  }
}
