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

public abstract class Message implements Encodable {

  public abstract Type type();

  public enum Type implements Encodable {
    UNKNOWN_TYPE(-1),
    RPC_RESPONSE(0);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() {
      return id;
    }

    @Override
    public int encodedLength() {
      return 1;
    }

    @Override
    public void encode(ByteBuf buf) {
      buf.writeByte(id);
    }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0:
          return RPC_RESPONSE;
        case -1:
          throw new IllegalArgumentException("User type messages cannot be decoded.");
        default:
          throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }

  public static Message decode(Type msgType, ByteBuf in) {
    switch (msgType) {
      case RPC_RESPONSE:
        return RpcResponse.decode(in);
      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
