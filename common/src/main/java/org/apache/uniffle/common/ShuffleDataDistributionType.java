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

package org.apache.uniffle.common;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.netty.EncodeException;
import org.apache.uniffle.common.netty.protocol.Encodable;

/**
 * The type of shuffle data distribution of a single partition.
 */
public enum ShuffleDataDistributionType implements Encodable {
  NORMAL(0),
  LOCAL_ORDER(1);

  private final byte id;

  ShuffleDataDistributionType(int id) {
    this.id = (byte) id;
  }

  @Override
  public int encodedLength() {
    return 1;
  }

  @Override
  public void encode(ByteBuf buf) throws EncodeException {
    buf.writeByte(id);
  }

  public static ShuffleDataDistributionType decode(ByteBuf buf) {
    byte id = buf.readByte();
    switch (id) {
      case 0:
        return NORMAL;
      case 1:
        return LOCAL_ORDER;
      default:
        throw new IllegalArgumentException("Unknown ShuffleDataDistributionType: " + id);
    }
  }
}
