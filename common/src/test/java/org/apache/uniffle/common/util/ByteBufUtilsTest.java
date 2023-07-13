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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ByteBufUtilsTest {

  @Test
  public void test() {
    ByteBuf byteBuf = Unpooled.buffer(100);
    String expectedString = "test_str";
    ByteBufUtils.writeLengthAndString(byteBuf, expectedString);
    assertEquals(expectedString, ByteBufUtils.readLengthAndString(byteBuf));

    byteBuf.clear();
    byte[] expectedBytes = expectedString.getBytes();
    byteBuf.writeBytes(expectedBytes);
    assertArrayEquals(expectedBytes, ByteBufUtils.readBytes(byteBuf));

    byteBuf.clear();
    ByteBufUtils.writeLengthAndString(byteBuf, null);
    assertNull(ByteBufUtils.readLengthAndString(byteBuf));

    byteBuf.clear();
    ByteBufUtils.writeLengthAndString(byteBuf, expectedString);
    ByteBuf byteBuf1 = Unpooled.buffer(100);
    ByteBufUtils.writeLengthAndString(byteBuf1, expectedString);
    final int expectedLength = byteBuf.readableBytes() + byteBuf1.readableBytes();
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true, byteBuf);
    compositeByteBuf.addComponent(true, byteBuf1);

    ByteBuf res = Unpooled.buffer(100);
    ByteBufUtils.copyByteBuf(compositeByteBuf, res);
    assertEquals(expectedLength, res.readableBytes() - Integer.BYTES);

    res.clear();
    ByteBufUtils.copyByteBuf(compositeByteBuf, res);
    assertEquals(expectedLength, res.readableBytes() - Integer.BYTES);

    byteBuf.clear();
    byte[] bytes = expectedString.getBytes(StandardCharsets.UTF_8);
    byteBuf.writeBytes(bytes);
    ByteBufUtils.readBytes(byteBuf, bytes, 1, byteBuf.readableBytes() - 1);
    assertEquals("ttest_st", new String(bytes, StandardCharsets.UTF_8));
  }
}
