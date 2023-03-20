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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ByteBufUtils;

public class Decoders {
  public static ShuffleServerInfo decodeShuffleServerInfo(ByteBuf byteBuf) {
    String id = ByteBufUtils.readLengthAndString(byteBuf);
    String host = ByteBufUtils.readLengthAndString(byteBuf);
    int grpcPort = byteBuf.readInt();
    int nettyPort = byteBuf.readInt();
    return new ShuffleServerInfo(id, host, grpcPort, nettyPort);
  }

  public static ShuffleBlockInfo decodeShuffleBlockInfo(ByteBuf byteBuf) {
    int partId = byteBuf.readInt();
    long blockId = byteBuf.readLong();
    int length = byteBuf.readInt();
    int shuffleId = byteBuf.readInt();
    long crc = byteBuf.readLong();
    long taskAttemptId = byteBuf.readLong();
    // todo: we can readSlice here, but it needs to be released manually after use, or it will cause a memory leak
    byte[] data = ByteBufUtils.readByteArray(byteBuf);
    int lengthOfShuffleServers = byteBuf.readInt();
    List<ShuffleServerInfo> serverInfos = Lists.newArrayList();
    for (int k = 0; k < lengthOfShuffleServers; k++) {
      serverInfos.add(decodeShuffleServerInfo(byteBuf));
    }
    int uncompressLength = byteBuf.readInt();
    long freeMemory = byteBuf.readLong();
    return new ShuffleBlockInfo(shuffleId, partId, blockId,
        length, crc, Unpooled.wrappedBuffer(data), serverInfos, uncompressLength, freeMemory, taskAttemptId);
  }
}
