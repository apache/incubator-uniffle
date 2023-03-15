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

package org.apache.uniffle.common.util;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class ByteBufUtils {

  public static final void writeLengthAndString(ByteBuf buf, String str) {
    if (str == null) {
      buf.writeInt(-1);
      return;
    }

    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    buf.writeInt(bytes.length);
    buf.writeBytes(bytes);
  }

  public static final String readLengthAndString(ByteBuf buf) {
    int length = buf.readInt();
    if (length == -1) {
      return null;
    }

    byte[] bytes = new byte[length];
    buf.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static final byte[] readBytes(ByteBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }
}
