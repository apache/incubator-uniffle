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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnitConverterTest {

  private static final long PB = (long)ByteUnit.PiB.toBytes(1L);
  private static final long TB = (long)ByteUnit.TiB.toBytes(1L);
  private static final long GB = (long)ByteUnit.GiB.toBytes(1L);
  private static final long MB = (long)ByteUnit.MiB.toBytes(1L);
  private static final long KB = (long)ByteUnit.KiB.toBytes(1L);

  @Test
  public void testByteString() {

    assertEquals(10 * PB, UnitConverter.byteStringAs("10PB", ByteUnit.BYTE));
    assertEquals(10 * PB, UnitConverter.byteStringAs("10pb", ByteUnit.BYTE));
    assertEquals(10 * PB, UnitConverter.byteStringAs("10pB", ByteUnit.BYTE));
    assertEquals(10 * PB, UnitConverter.byteStringAs("10p", ByteUnit.BYTE));
    assertEquals(10 * PB, UnitConverter.byteStringAs("10P", ByteUnit.BYTE));

    assertEquals(10 * TB, UnitConverter.byteStringAs("10TB", ByteUnit.BYTE));
    assertEquals(10 * TB, UnitConverter.byteStringAs("10tb", ByteUnit.BYTE));
    assertEquals(10 * TB, UnitConverter.byteStringAs("10tB", ByteUnit.BYTE));
    assertEquals(10 * TB, UnitConverter.byteStringAs("10T", ByteUnit.BYTE));
    assertEquals(10 * TB, UnitConverter.byteStringAs("10t", ByteUnit.BYTE));

    assertEquals(10 * GB, UnitConverter.byteStringAs("10GB", ByteUnit.BYTE));
    assertEquals(10 * GB, UnitConverter.byteStringAs("10gb", ByteUnit.BYTE));
    assertEquals(10 * GB, UnitConverter.byteStringAs("10gB", ByteUnit.BYTE));

    assertEquals(10 * MB, UnitConverter.byteStringAs("10MB", ByteUnit.BYTE));
    assertEquals(10 * MB, UnitConverter.byteStringAs("10mb", ByteUnit.BYTE));
    assertEquals(10 * MB, UnitConverter.byteStringAs("10mB", ByteUnit.BYTE));
    assertEquals(10 * MB, UnitConverter.byteStringAs("10M", ByteUnit.BYTE));
    assertEquals(10 * MB, UnitConverter.byteStringAs("10m", ByteUnit.BYTE));

    assertEquals(10 * KB, UnitConverter.byteStringAs("10KB", ByteUnit.BYTE));
    assertEquals(10 * KB, UnitConverter.byteStringAs("10kb", ByteUnit.BYTE));
    assertEquals(10 * KB, UnitConverter.byteStringAs("10Kb", ByteUnit.BYTE));
    assertEquals(10 * KB, UnitConverter.byteStringAs("10K", ByteUnit.BYTE));
    assertEquals(10 * KB, UnitConverter.byteStringAs("10k", ByteUnit.BYTE));

    assertEquals(1111, UnitConverter.byteStringAs("1111", ByteUnit.BYTE));
  }
}
