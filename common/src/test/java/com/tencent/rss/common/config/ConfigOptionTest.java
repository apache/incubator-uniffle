/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.common.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.function.Function;

public class ConfigOptionTest {

  @Test
  public void testListTypes() {
    // test the string type list.
    final ConfigOption<List<String>> listStringConfigOption = ConfigOptions
            .key("rss.key1")
            .stringType()
            .asList()
            .defaultValues("h1", "h2")
            .withDescription("List config key1");

    List<String> defaultValues = listStringConfigOption.defaultValue();
    assertEquals(2, defaultValues.size());
    assertSame(String.class, listStringConfigOption.getClazz());

    RssBaseConf conf = new RssBaseConf();
    conf.set(listStringConfigOption, "a,b,c");

    List<String> vals = conf.get(listStringConfigOption);
    assertEquals(3, vals.size());
    assertEquals(vals.toString(), "[a, b, c]");

    // test the long type list
    final ConfigOption<List<Long>> listLongConfigOption = ConfigOptions
            .key("rss.key2")
            .longType()
            .asList()
            .defaultValues(1)
            .withDescription("List long config key2");

    List<Long> longDefaultVals = listLongConfigOption.defaultValue();
    assertEquals(longDefaultVals.size(), 1);

    conf.set(listLongConfigOption, "1,2,3");
    List<Long> longVals = conf.get(listLongConfigOption);
    assertEquals("[1, 2, 3]", longVals.toString());
    assertEquals(1, longVals.get(0));

    // test the no-default values
    final ConfigOption<List<Long>> listLongConfigOptionWithoutDefault = ConfigOptions
            .key("rss.key3")
            .longType()
            .asList()
            .noDefaultValue()
            .withDescription("List long config key3 without default values");
    List<Long> valsWithoutDefault = listLongConfigOptionWithoutDefault.defaultValue();
    assertNull(valsWithoutDefault);

    // test the method of check
    final ConfigOption<List<Integer>> checkLongValsOptions = ConfigOptions
            .key("rss.key4")
            .intType()
            .asList()
            .checkValue((Function<Integer, Boolean>) val -> val > 0, "Every number of list should be positive")
            .noDefaultValue()
            .withDescription("The key4 is illegal");

    conf.set(checkLongValsOptions, "-1,2,3");

    try {
      conf.get(checkLongValsOptions);
      fail();
    } catch (IllegalArgumentException illegalArgumentException) {
    }

    conf.set(checkLongValsOptions, "1,2,3");
    try {
      conf.get(checkLongValsOptions);
    } catch (IllegalArgumentException illegalArgumentException) {
      fail();
    }
  }

  @Test
  public void testBasicTypes() {
    final ConfigOption<Integer> intConfig = ConfigOptions
        .key("rss.key1")
        .intType()
        .defaultValue(1000)
        .withDescription("Int config key1");
    assertSame(Integer.class, intConfig.getClazz());
    assertEquals(1000, (int) intConfig.defaultValue());
    assertEquals("Int config key1", intConfig.description());

    final ConfigOption<Long> longConfig = ConfigOptions
        .key("rss.key2")
        .longType()
        .defaultValue(1999L);
    assertTrue(longConfig.hasDefaultValue());
    assertEquals(1999L, (long) longConfig.defaultValue());

    final ConfigOption<String> stringConfig = ConfigOptions
        .key("rss.key3")
        .stringType()
        .noDefaultValue();
    assertFalse(stringConfig.hasDefaultValue());
    assertEquals("", stringConfig.description());

    final ConfigOption<Boolean> booleanConfig = ConfigOptions
        .key("key4")
        .booleanType()
        .defaultValue(false)
        .withDescription("Boolean config key");
    assertFalse(booleanConfig.defaultValue());
    assertEquals("Boolean config key", booleanConfig.description());

    final ConfigOption<Integer> positiveInt = ConfigOptions
        .key("key5")
        .intType()
        .checkValue((v) -> {return v > 0;}, "The value of key5 must be positive")
        .defaultValue(1)
        .withDescription("Positive integer key");
    RssBaseConf conf = new RssBaseConf();
    conf.set(positiveInt, -1);
    boolean isException = false;
    try {
      conf.get(positiveInt);
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("The value of key5 must be positive"));
    }
    assertTrue(isException);
    conf.set(positiveInt, 1);
    try {
      conf.get(positiveInt);
    } catch (IllegalArgumentException ie) {
      fail();
    }
  }
}
