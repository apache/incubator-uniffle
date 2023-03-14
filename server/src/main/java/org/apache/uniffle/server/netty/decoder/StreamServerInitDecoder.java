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

package org.apache.uniffle.server.netty.decoder;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.server.ShuffleServer;

public class StreamServerInitDecoder extends ByteToMessageDecoder {

  private static final Logger logger = LoggerFactory.getLogger(StreamServerInitDecoder.class);

  private ShuffleServer shuffleServer;

  public StreamServerInitDecoder(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  private void addDecoder(ChannelHandlerContext ctx, byte type) {

  }

  @Override
  protected void decode(ChannelHandlerContext ctx,
      ByteBuf in,
      List<Object> out) {
    if (in.readableBytes() < Byte.BYTES) {
      return;
    }
    in.markReaderIndex();
    byte magicByte = in.readByte();
    in.resetReaderIndex();

    addDecoder(ctx, magicByte);
  }

}
