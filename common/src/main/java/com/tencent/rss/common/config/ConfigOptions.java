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

package com.tencent.rss.common.config;

import java.util.Objects;
import java.util.function.Function;

/**
 * {@code ConfigOptions} are used to build a {@link ConfigOption}.
 * The option is typically built in one of the following pattern:
 *
 * <pre>{@code
 * // simple string-valued option with a default value
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // simple integer-valued option with a default value
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("application.parallelism")
 *     .intType()
 *     .defaultValue(100);
 *
 * // option with no default value
 * ConfigOption<String> userName = ConfigOptions
 *     .key("user.name")
 *     .stringType()
 *     .noDefaultValue();
 *
 * // simple positive-integer-valued option with a default value
 * ConfigOption<Integer> threadNum = ConfigOptions
 *     .key("thread.num")
 *     .intType()
 *     .checkValue("The value of 'thread.num' must be positive")
 *     .defaultValue(10);
 *
 * }</pre>
 */
public class ConfigOptions {
  /**
   * Not intended to be instantiated.
   */
  private ConfigOptions() {
  }

  // ------------------------------------------------------------------------

  /**
   * Starts building a new {@link ConfigOption}.
   *
   * @param key The key for the config option.
   * @return The builder for the config option with the given key.
   */
  public static OptionBuilder key(String key) {
    Objects.requireNonNull(key);
    return new OptionBuilder(key);
  }

  /**
   * The option builder is used to create a {@link ConfigOption}.
   * It is instantiated via {@link ConfigOptions#key(String)}.
   */
  public static final class OptionBuilder {
    /**
     * The key for the config option.
     */
    private final String key;

    /**
     * Creates a new OptionBuilder.
     *
     * @param key The key for the config option
     */
    OptionBuilder(String key) {
      this.key = key;
    }

    /**
     * Defines that the value of the option should be of {@link Boolean} type.
     */
    public TypedConfigOptionBuilder<Boolean> booleanType() {
      return new TypedConfigOptionBuilder<>(key, Boolean.class);
    }

    /**
     * Defines that the value of the option should be of {@link Integer} type.
     */
    public TypedConfigOptionBuilder<Integer> intType() {
      return new TypedConfigOptionBuilder<>(key, Integer.class);
    }

    /**
     * Defines that the value of the option should be of {@link Long} type.
     */
    public TypedConfigOptionBuilder<Long> longType() {
      return new TypedConfigOptionBuilder<>(key, Long.class);
    }

    /**
     * Defines that the value of the option should be of {@link Float} type.
     */
    public TypedConfigOptionBuilder<Float> floatType() {
      return new TypedConfigOptionBuilder<>(key, Float.class);
    }

    /**
     * Defines that the value of the option should be of {@link Double} type.
     */
    public TypedConfigOptionBuilder<Double> doubleType() {
      return new TypedConfigOptionBuilder<>(key, Double.class);
    }

    /**
     * Defines that the value of the option should be of {@link String} type.
     */
    public TypedConfigOptionBuilder<String> stringType() {
      return new TypedConfigOptionBuilder<>(key, String.class);
    }
  }

  // ------------------------------------------------------------------------

  /**
   * Builder for {@link ConfigOption} with a defined atomic type.
   *
   * @param <T> atomic type of the option
   */
  public static class TypedConfigOptionBuilder<T> {
    private final String key;
    private final Class<T> clazz;
    private final Function<Object, T> converter;

    TypedConfigOptionBuilder(String key, Class<T> clazz) {
      this.key = key;
      this.clazz = clazz;
      this.converter = (v) -> {
        try {
          return ConfigUtils.convertValue(v, clazz);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format(
              "Could not parse value '%s' for key '%s'.", v.toString(),
              key), e);
        }
      };
    }

    TypedConfigOptionBuilder(String key, Class<T> clazz, Function<Object, T> converter) {
      this.key = key;
      this.clazz = clazz;
      this.converter = converter;
    }

    // todo: errorMsg shouldn't contain key
    public TypedConfigOptionBuilder checkValue(Function<T, Boolean> checkValue, String errMsg) {
      Function<Object, T> newConverter = (v) -> {
        T newValue = this.converter.apply(v);
        if (!checkValue.apply(newValue)) {
          throw new IllegalArgumentException(errMsg);
        }
        return newValue;
      };
      return new TypedConfigOptionBuilder(key, clazz, newConverter);
    }

    /**
     * Creates a ConfigOption with the given default value.
     *
     * @param value The default value for the config option
     * @return The config option with the default value.
     */
    public ConfigOption<T> defaultValue(T value) {
      return new ConfigOption<>(
        key,
        clazz,
        ConfigOption.EMPTY_DESCRIPTION,
        value,
        converter);
    }

    /**
     * Creates a ConfigOption without a default value.
     *
     * @return The config option without a default value.
     */
    public ConfigOption<T> noDefaultValue() {
      return new ConfigOption<>(
        key,
        clazz,
        ConfigOption.EMPTY_DESCRIPTION,
        null,
        converter);
    }
  }
}
