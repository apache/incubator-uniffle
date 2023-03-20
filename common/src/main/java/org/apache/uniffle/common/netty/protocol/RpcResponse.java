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

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class RpcResponse extends Message {
  private long requestId;
  private StatusCode statusCode;
  private String retMessage;

  public RpcResponse(long requestId, StatusCode statusCode) {
    this(requestId, statusCode, null);
  }

  public RpcResponse(long requestId, StatusCode statusCode, String retMessage) {
    this.requestId = requestId;
    this.statusCode = statusCode;
    this.retMessage = retMessage;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public String getRetMessage() {
    return retMessage;
  }

  @Override
  public String toString() {
    return "RpcResponse{"
        + "requestId=" + requestId
        + ", statusCode=" + statusCode
        + ", retMessage='" + retMessage
        + '\'' + '}';
  }

  @Override
  public int encodedLength() {
    return Long.BYTES + Integer.BYTES + ByteBufUtils.encodedLength(retMessage);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeInt(statusCode.ordinal());
    ByteBufUtils.writeLengthAndString(buf, retMessage);
  }


  public static RpcResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    StatusCode statusCode = StatusCode.fromCode(buf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(buf);
    return new RpcResponse(requestId, statusCode, retMessage);
  }

  public long getRequestId() {
    return requestId;
  }

  @Override
  public Type type() {
    return Type.RPC_RESPONSE;
  }
}
