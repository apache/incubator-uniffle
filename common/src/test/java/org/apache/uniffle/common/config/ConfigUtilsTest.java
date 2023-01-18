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

package org.apache.uniffle.common.config;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigUtilsTest {

  private enum Ternary {
    TRUE, FALSE, UNKNOWN
  }

  private static Stream<Arguments> convertValueArgs() {
    return Stream.of(
        Arguments.arguments(1, Integer.class),
        Arguments.arguments(2L, Long.class),
        Arguments.arguments(true, Boolean.class),
        Arguments.arguments(1.0f, Float.class),
        Arguments.arguments(2.0, Double.class),
        Arguments.arguments("foo", String.class),
        Arguments.arguments(Ternary.FALSE, Ternary.class)
    );
  }

  @ParameterizedTest
  @MethodSource("convertValueArgs")
  public void testConvertValue(Object rawValue, Class<?> clazz) {
    assertEquals(rawValue, ConfigUtils.convertValue(rawValue, clazz));
  }

  @Test
  public void testConvertValueWithUnsupportedType() {
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertValue(0, Object.class));
  }

  @Test
  public void testConvertToString() {
    assertEquals("foo", ConfigUtils.convertToString("foo"));
    assertEquals("123", ConfigUtils.convertToString(123));
  }

  @Test
  public void testConvertToInt() {
    final long minInt = Integer.MIN_VALUE;
    final long maxInt = Integer.MAX_VALUE;
    assertEquals(Integer.MIN_VALUE, ConfigUtils.convertToInt(Integer.MIN_VALUE));
    assertEquals(Integer.MAX_VALUE, ConfigUtils.convertToInt(Integer.MAX_VALUE));
    assertEquals(Integer.MIN_VALUE, ConfigUtils.convertToInt(minInt));
    assertEquals(Integer.MAX_VALUE, ConfigUtils.convertToInt(maxInt));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToInt(maxInt + 1));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToInt(minInt - 1));
    assertEquals(123, ConfigUtils.convertToInt("123"));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToInt("foo"));
  }

  @Test
  public void testConvertToLong() {
    assertEquals(Integer.MIN_VALUE, ConfigUtils.convertToLong(Integer.MIN_VALUE));
    assertEquals(Integer.MAX_VALUE, ConfigUtils.convertToLong(Integer.MAX_VALUE));
    assertEquals(Long.MIN_VALUE, ConfigUtils.convertToLong(Long.MIN_VALUE));
    assertEquals(Long.MAX_VALUE, ConfigUtils.convertToLong(Long.MAX_VALUE));
    assertEquals(123, ConfigUtils.convertToLong("123"));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToLong("foo"));
  }

  private static Stream<Arguments> convertToSizeArgs() {
    return Stream.of(
        Arguments.arguments(0, 0),
        Arguments.arguments(12345L, 12345),
        Arguments.arguments("100", 100),
        Arguments.arguments("100b", 100),
        Arguments.arguments("2K", 2L << 10),
        Arguments.arguments("2Kb", 2L << 10),
        Arguments.arguments("3M", 3L << 20),
        Arguments.arguments("3mB", 3L << 20),
        Arguments.arguments("4G", 4L << 30),
        Arguments.arguments("4GB", 4L << 30),
        Arguments.arguments("5T", 5L << 40),
        Arguments.arguments("5tb", 5L << 40),
        Arguments.arguments("6P", 6L << 50),
        Arguments.arguments("6PB", 6L << 50)
    );
  }

  @ParameterizedTest
  @MethodSource("convertToSizeArgs")
  public void testConvertToSizeInBytes(Object size, long expected) {
    assertEquals(expected, ConfigUtils.convertToSizeInBytes(size));
    if (size instanceof String) {
      assertEquals(expected, ConfigUtils.convertToSizeInBytes(((String) size).toLowerCase()));
      assertEquals(expected, ConfigUtils.convertToSizeInBytes(((String) size).toUpperCase()));
    }
  }

  @Test
  public void testConvertToBoolean() {
    assertEquals(true, ConfigUtils.convertToBoolean(true));
    assertEquals(false, ConfigUtils.convertToBoolean(false));
    assertEquals(true, ConfigUtils.convertToBoolean("True"));
    assertEquals(false, ConfigUtils.convertToBoolean("False"));
    assertEquals(true, ConfigUtils.convertToBoolean("true"));
    assertEquals(false, ConfigUtils.convertToBoolean("false"));
    assertEquals(true, ConfigUtils.convertToBoolean("TRUE"));
    assertEquals(false, ConfigUtils.convertToBoolean("FALSE"));
    assertEquals(true, ConfigUtils.convertToBoolean("TRUe"));
    assertEquals(false, ConfigUtils.convertToBoolean("fAlsE"));
    assertEquals(true, ConfigUtils.convertToBoolean(Ternary.TRUE));
    assertEquals(false, ConfigUtils.convertToBoolean(Ternary.FALSE));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToBoolean(Ternary.UNKNOWN));
  }

  @Test
  public void testConvertToFloat() {
    final double minFloat = Float.MIN_VALUE;
    final double maxFloat = Float.MAX_VALUE;
    assertEquals(Float.MIN_VALUE, ConfigUtils.convertToFloat(Float.MIN_VALUE));
    assertEquals(Float.MAX_VALUE, ConfigUtils.convertToFloat(Float.MAX_VALUE));
    assertEquals(Float.MIN_VALUE, ConfigUtils.convertToFloat(minFloat));
    assertEquals(Float.MAX_VALUE, ConfigUtils.convertToFloat(maxFloat));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToFloat(maxFloat * 1.1));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToFloat(minFloat * 0.9));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToFloat(-maxFloat * 1.1));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToFloat(-minFloat * 0.9));
    assertEquals(123.45f, ConfigUtils.convertToFloat("123.45"));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToFloat("foo"));
  }

  @Test
  public void testConvertToDouble() {
    assertEquals(Float.MIN_VALUE, ConfigUtils.convertToDouble(Float.MIN_VALUE));
    assertEquals(Float.MAX_VALUE, ConfigUtils.convertToDouble(Float.MAX_VALUE));
    assertEquals(Double.MIN_VALUE, ConfigUtils.convertToDouble(Double.MIN_VALUE));
    assertEquals(Double.MAX_VALUE, ConfigUtils.convertToDouble(Double.MAX_VALUE));
    assertEquals(123.45, ConfigUtils.convertToDouble("123.45"));
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.convertToDouble("foo"));
  }

  @Test
  public void testGetAllConfigOptions() {
    assertFalse(ConfigUtils.getAllConfigOptions(RssBaseConf.class).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 2, -1, Long.MIN_VALUE, Long.MAX_VALUE})
  public void testPositiveLongValidator(long value) {
    assertEquals(value > 0, ConfigUtils.POSITIVE_LONG_VALIDATOR.apply(value));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 2, -1, Long.MIN_VALUE, Long.MAX_VALUE})
  public void testNonNegativeLongValidator(long value) {
    assertEquals(value >= 0, ConfigUtils.NON_NEGATIVE_LONG_VALIDATOR.apply(value));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 2, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
  public void testPositiveIntegerValidator(long value) {
    assertEquals(value > 0 && value <= Integer.MAX_VALUE, ConfigUtils.POSITIVE_INTEGER_VALIDATOR.apply(value));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, -1, Integer.MIN_VALUE, Integer.MAX_VALUE})
  public void testPositiveIntegerValidator2(int value) {
    assertEquals(value > 0, ConfigUtils.POSITIVE_INTEGER_VALIDATOR_2.apply(value));
  }

  @ParameterizedTest
  @ValueSource(doubles = {-1.0, -0.01, 0.0, 1.5, 50.2, 99.9, 100.0, 100.01, 101.0})
  public void testPercentageDoubleValidator(double value) {
    assertEquals(value >= 0.0 && value <= 100.0, ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR.apply(value));
  }

}
