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

package org.apache.uniffle.flink.reader;

import java.nio.ByteBuffer;
import java.util.Queue;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.flink.buffer.WriteBufferPacker;

public class RssFetcher implements Runnable {
  private Queue<ChannelBuffer> receivedBuffers;
  private ShuffleReadClient client;
  private boolean stopped = false;
  private InputChannelInfo inputChannelInfo;

  public RssFetcher(
      Queue<ChannelBuffer> receivedBuffers,
      ShuffleReadClient client,
      InputChannelInfo inputChannelInfo) {
    this.receivedBuffers = receivedBuffers;
    this.client = client;
    this.inputChannelInfo = inputChannelInfo;
  }

  public void fetchAllBlocks() {
    while (!stopped) {
      try {
        copyFromRssServer();
      } finally {
      }
    }
  }

  public void copyFromRssServer() {
    CompressedShuffleBlock shuffleBlock = client.readShuffleBlockData();
    if (shuffleBlock != null) {
      ByteBuffer byteBuffer = shuffleBlock.getByteBuffer();
      if (byteBuffer != null) {
        ByteBuf byteBuf = byteBuffer2NettyBuffers(byteBuffer);
        Queue<Buffer> unpackBuffers = WriteBufferPacker.unpack(byteBuf);
        while (!unpackBuffers.isEmpty()) {
          fetchBuffer2receivedBuffers(unpackBuffers.poll());
        }
      } else {
        client.checkProcessedBlockIds();
        client.logStatics();
        client.close();
        stopped = true;
      }
    }
  }

  private ByteBuf byteBuffer2NettyBuffers(ByteBuffer byteBuffer) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer();
    byteBuf.writeBytes(byteBuffer.array());
    return byteBuf;
  }

  private void fetchBuffer2receivedBuffers(Buffer buffer) {
    synchronized (receivedBuffers) {
      receivedBuffers.add(new ChannelBuffer(buffer, inputChannelInfo));
    }
  }

  @Override
  public void run() {
    fetchAllBlocks();
  }
}
