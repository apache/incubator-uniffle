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
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetLocalShuffleIndexResponse extends RpcResponse {
  private ByteBuf indexData;
  private long fileLength;

  public GetLocalShuffleIndexResponse(long requestId, StatusCode statusCode, byte[] indexData, long fileLength) {
    this(requestId, statusCode, null, indexData, fileLength);
  }

  public GetLocalShuffleIndexResponse(long requestId, StatusCode statusCode, String retMessage,
      byte[] indexData, long fileLength) {
    this(requestId, statusCode, retMessage, Unpooled.wrappedBuffer(indexData), fileLength);
  }

  public GetLocalShuffleIndexResponse(long requestId, StatusCode statusCode, String retMessage,
      ByteBuf indexData, long fileLength) {
    super(requestId, statusCode, retMessage);
    this.indexData = indexData;
    this.fileLength = fileLength;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Integer.BYTES + indexData.readableBytes() + Long.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    ByteBufUtils.copyByteBuf(indexData, buf);
    buf.writeLong(fileLength);
  }

  public static GetLocalShuffleIndexResponse decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    ByteBuf indexData = ByteBufUtils.readSlice(byteBuf);
    long fileLength = byteBuf.readLong();
    return new GetLocalShuffleIndexResponse(requestId, statusCode, retMessage, indexData, fileLength);
  }

  @Override
  public Type type() {
    return Type.GET_LOCAL_SHUFFLE_INDEX_RESPONSE;
  }

  public ByteBuf getIndexData() {
    return indexData;
  }

  public long getFileLength() {
    return fileLength;
  }
}
