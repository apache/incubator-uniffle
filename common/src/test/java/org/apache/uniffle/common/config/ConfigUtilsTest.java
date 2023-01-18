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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigUtilsTest {

  private enum ExampleEnum {
    FOO, BAR, BAZ
  }

  private static Stream<Arguments> convertValueArgs() {
    return Stream.of(
        Arguments.arguments(1, Integer.class),
        Arguments.arguments(2L, Long.class),
        Arguments.arguments(true, Boolean.class),
        Arguments.arguments(1.0f, Float.class),
        Arguments.arguments(2.0, Double.class),
        Arguments.arguments("foo", String.class),
        Arguments.arguments(ExampleEnum.BAR, ExampleEnum.class)
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

}
