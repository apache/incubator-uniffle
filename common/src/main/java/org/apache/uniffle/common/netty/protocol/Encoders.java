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

import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleSegment;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ByteBufUtils;

public class Encoders {
  public static void encodeShuffleServerInfo(ShuffleServerInfo shuffleServerInfo, ByteBuf byteBuf) {
    ByteBufUtils.writeLengthAndString(byteBuf, shuffleServerInfo.getId());
    ByteBufUtils.writeLengthAndString(byteBuf, shuffleServerInfo.getHost());
    byteBuf.writeInt(shuffleServerInfo.getGrpcPort());
    byteBuf.writeInt(shuffleServerInfo.getNettyPort());
  }

  public static void encodeShuffleBlockInfo(ShuffleBlockInfo shuffleBlockInfo, ByteBuf byteBuf) {
    byteBuf.writeInt(shuffleBlockInfo.getPartitionId());
    byteBuf.writeLong(shuffleBlockInfo.getBlockId());
    byteBuf.writeInt(shuffleBlockInfo.getLength());
    byteBuf.writeInt(shuffleBlockInfo.getShuffleId());
    byteBuf.writeLong(shuffleBlockInfo.getCrc());
    byteBuf.writeLong(shuffleBlockInfo.getTaskAttemptId());
    // todo: avoid copy
    shuffleBlockInfo.copyDataTo(byteBuf);
    List<ShuffleServerInfo> shuffleServerInfoList = shuffleBlockInfo.getShuffleServerInfos();
    byteBuf.writeInt(shuffleServerInfoList.size());
    for (ShuffleServerInfo shuffleServerInfo : shuffleServerInfoList) {
      Encoders.encodeShuffleServerInfo(shuffleServerInfo, byteBuf);
    }
    byteBuf.writeInt(shuffleBlockInfo.getUncompressLength());
    byteBuf.writeLong(shuffleBlockInfo.getFreeMemory());
  }

  public static int encodeLengthOfShuffleServerInfo(ShuffleServerInfo shuffleServerInfo) {
    return ByteBufUtils.encodedLength(shuffleServerInfo.getId())
        + ByteBufUtils.encodedLength(shuffleServerInfo.getHost())
        + 2 * Integer.BYTES;
  }

  public static int encodeLengthOfShuffleBlockInfo(ShuffleBlockInfo shuffleBlockInfo) {
    int encodeLength =
        4 * Long.BYTES
            + 4 * Integer.BYTES
            + Integer.BYTES
            + shuffleBlockInfo.getLength()
            + Integer.BYTES;
    for (ShuffleServerInfo shuffleServerInfo : shuffleBlockInfo.getShuffleServerInfos()) {
      encodeLength += encodeLengthOfShuffleServerInfo(shuffleServerInfo);
    }
    return encodeLength;
  }

  public static void encodePartitionRanges(List<PartitionRange> partitionRanges, ByteBuf byteBuf) {
    byteBuf.writeInt(partitionRanges.size());
    for (PartitionRange partitionRange : partitionRanges) {
      byteBuf.writeInt(partitionRange.getStart());
      byteBuf.writeInt(partitionRange.getEnd());
    }
  }

  public static void encodeBufferSegments(List<ShuffleSegment> shuffleSegments, ByteBuf byteBuf) {
    byteBuf.writeInt(shuffleSegments.size());
    for (ShuffleSegment shuffleSegment : shuffleSegments) {
      byteBuf.writeLong(shuffleSegment.getBlockId());
      byteBuf.writeInt(shuffleSegment.getOffset());
      byteBuf.writeInt(shuffleSegment.getLength());
      byteBuf.writeInt(shuffleSegment.getUncompressLength());
      byteBuf.writeLong(shuffleSegment.getCrc());
      byteBuf.writeLong(shuffleSegment.getTaskAttemptId());
    }
  }

  public static int encodeLengthOfBufferSegments(List<ShuffleSegment> shuffleSegments) {
    return Integer.BYTES + shuffleSegments.size() * (3 * Long.BYTES + 3 * Integer.BYTES);
  }
}
