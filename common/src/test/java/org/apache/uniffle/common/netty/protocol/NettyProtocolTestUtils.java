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
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public class NettyProtocolTestUtils {

  static class SendShuffleDataRequestTest extends SendShuffleDataRequest {
    SendShuffleDataRequestTest(long requestId, String appId, int shuffleId, long requireId,
        Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks, long timestamp) {
      super(requestId, appId, shuffleId, requireId, partitionToBlocks, timestamp);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SendShuffleDataRequestTest that = (SendShuffleDataRequestTest) o;
      Map<Integer, List<ShuffleBlockInfoTest>> map1 = Maps.newHashMap();
      Map<Integer, List<ShuffleBlockInfoTest>> map2 = Maps.newHashMap();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : this.getPartitionToBlocks().entrySet()) {
        map1.put(entry.getKey(), toShuffleBlockInfoTestList(entry.getValue()));
      }
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : that.getPartitionToBlocks().entrySet()) {
        map2.put(entry.getKey(), toShuffleBlockInfoTestList(entry.getValue()));
      }
      return this.requestId == that.requestId
                 && this.getShuffleId() == that.getShuffleId()
                 && this.getRequireId() == that.getRequireId()
                 && this.getTimestamp() == that.getTimestamp()
                 && this.getAppId().equals(that.getAppId())
                 && map1.equals(map2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(requestId, this.getAppId(), this.getShuffleId(), this.getRequireId(),
          this.getPartitionToBlocks(), this.getTimestamp());
    }
  }

  static class ShuffleBlockInfoTest extends ShuffleBlockInfo {
    ShuffleBlockInfoTest(int shuffleId, int partitionId, long blockId, int length, long crc,
        ByteBuf data, List<ShuffleServerInfo> shuffleServerInfos, int uncompressLength,
        long freeMemory, long taskAttemptId) {
      super(shuffleId, partitionId, blockId, length, crc, data, shuffleServerInfos, uncompressLength,
          freeMemory, taskAttemptId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ShuffleBlockInfoTest that = (ShuffleBlockInfoTest) o;
      return this.getPartitionId() == that.getPartitionId()
                 && this.getBlockId() == that.getBlockId()
                 && this.getLength() == that.getLength()
                 && this.getShuffleId() == that.getShuffleId()
                 && this.getCrc() == that.getCrc()
                 && this.getTaskAttemptId() == that.getTaskAttemptId()
                 && this.getUncompressLength() == that.getUncompressLength()
                 && this.getFreeMemory() == that.getFreeMemory()
                 && this.getData().equals(that.getData())
                 && this.getShuffleServerInfos().equals(that.getShuffleServerInfos());
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.getPartitionId(), this.getBlockId(), this.getLength(), this.getShuffleId(),
          this.getCrc(), this.getTaskAttemptId(), this.getData(), this.getShuffleServerInfos(),
          this.getUncompressLength(), this.getFreeMemory());
    }
  }

  private static SendShuffleDataRequestTest toSendShuffleDataRequestTest(
      SendShuffleDataRequest sendShuffleDataRequest) {
    return new SendShuffleDataRequestTest(sendShuffleDataRequest.requestId,
        sendShuffleDataRequest.getAppId(),
        sendShuffleDataRequest.getShuffleId(),
        sendShuffleDataRequest.getRequireId(),
        sendShuffleDataRequest.getPartitionToBlocks(),
        sendShuffleDataRequest.getTimestamp());
  }

  private static List<ShuffleBlockInfoTest> toShuffleBlockInfoTestList(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    List<ShuffleBlockInfoTest> res = Lists.newArrayList();
    for (ShuffleBlockInfo shuffleBlockInfo : shuffleBlockInfoList) {
      res.add(toShuffleBlockInfoTest(shuffleBlockInfo));
    }
    return res;
  }

  private static ShuffleBlockInfoTest toShuffleBlockInfoTest(ShuffleBlockInfo shuffleBlockInfo) {
    return new ShuffleBlockInfoTest(shuffleBlockInfo.getShuffleId(),
        shuffleBlockInfo.getPartitionId(),
        shuffleBlockInfo.getBlockId(),
        shuffleBlockInfo.getLength(),
        shuffleBlockInfo.getCrc(),
        shuffleBlockInfo.getData(),
        shuffleBlockInfo.getShuffleServerInfos(),
        shuffleBlockInfo.getUncompressLength(),
        shuffleBlockInfo.getFreeMemory(),
        shuffleBlockInfo.getTaskAttemptId());
  }

  public static boolean compareSendShuffleDataRequest(SendShuffleDataRequest req1,
      SendShuffleDataRequest req2) {
    return toSendShuffleDataRequestTest(req1).equals(toSendShuffleDataRequestTest(req2));
  }
}
