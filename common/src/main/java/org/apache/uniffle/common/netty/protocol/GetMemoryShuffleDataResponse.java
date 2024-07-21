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
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.ShuffleSegment;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetMemoryShuffleDataResponse extends RpcResponse {
  private List<ShuffleSegment> shuffleSegments;

  public GetMemoryShuffleDataResponse(
      long requestId, StatusCode statusCode, List<ShuffleSegment> shuffleSegments, byte[] data) {
    this(requestId, statusCode, null, shuffleSegments, data);
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<ShuffleSegment> shuffleSegments,
      byte[] data) {
    this(requestId, statusCode, retMessage, shuffleSegments, Unpooled.wrappedBuffer(data));
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<ShuffleSegment> shuffleSegments,
      ByteBuf data) {
    this(requestId, statusCode, retMessage, shuffleSegments, new NettyManagedBuffer(data));
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<ShuffleSegment> shuffleSegments,
      ManagedBuffer managedBuffer) {
    super(requestId, statusCode, retMessage, managedBuffer);
    this.shuffleSegments = shuffleSegments;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Encoders.encodeLengthOfBufferSegments(shuffleSegments);
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    Encoders.encodeBufferSegments(shuffleSegments, buf);
  }

  public static GetMemoryShuffleDataResponse decode(ByteBuf byteBuf, boolean decodeBody) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    List<ShuffleSegment> shuffleSegments = Decoders.decodeBufferSegments(byteBuf);
    if (decodeBody) {
      NettyManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
      return new GetMemoryShuffleDataResponse(
          requestId, statusCode, retMessage, shuffleSegments, nettyManagedBuffer);
    } else {
      return new GetMemoryShuffleDataResponse(
          requestId, statusCode, retMessage, shuffleSegments, NettyManagedBuffer.EMPTY_BUFFER);
    }
  }

  @Override
  public Type type() {
    return Type.GET_MEMORY_SHUFFLE_DATA_RESPONSE;
  }

  public List<ShuffleSegment> getBufferSegments() {
    return shuffleSegments;
  }
}
