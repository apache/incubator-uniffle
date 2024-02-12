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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Maps;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

/** Class to create {@link BlockIdLayout} from any kinds of config. */
public class BlockIdLayoutConfig {
  /** The keys are configured in the config of BLOCKID_LAYOUT. */
  public static final String SEQUENCE_ID_LENGTH = "sequenceIdLength";

  public static final String PARTITION_ID_LENGTH = "partitionIdLength";
  public static final String TASK_ATTEMPT_ID_LENGTH = "taskAttemptIdLength";

  /** The default configuration. */
  public static Map<String, String> DEFAULT;

  static {
    DEFAULT = Maps.newHashMap();
    DEFAULT.put(SEQUENCE_ID_LENGTH, "18");
    DEFAULT.put(PARTITION_ID_LENGTH, "24");
    DEFAULT.put(TASK_ATTEMPT_ID_LENGTH, "21");
  }

  public static boolean validate(Map<String, String> config) {
    return Stream.of(SEQUENCE_ID_LENGTH, PARTITION_ID_LENGTH, TASK_ATTEMPT_ID_LENGTH)
        .allMatch(key -> config.containsKey(key) && config.get(key).matches("^[0-9]+$"));
  }

  private static Integer parseFieldLength(String key, Map<String, String> config) {
    if (!config.containsKey(key)) {
      throw new IllegalArgumentException("Config must contain key " + key);
    }

    String stringValue = config.get(key);
    try {
      return Integer.parseInt(stringValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Value of key " + key + " must be an integer: " + stringValue, e);
    }
  }

  public static BlockIdLayout apply(RssConf rssConf) {
    Map<String, String> config = rssConf.get(RssClientConf.BLOCKID_LAYOUT);
    if (config == null) {
      throw new IllegalArgumentException(
          "Config key " + RssClientConf.BLOCKID_LAYOUT.key() + " is missing");
    }
    return apply(config);
  }

  public static BlockIdLayout apply(Map<String, String> config) {
    List<Integer> fieldLengths =
        Stream.of(SEQUENCE_ID_LENGTH, PARTITION_ID_LENGTH, TASK_ATTEMPT_ID_LENGTH)
            .map(key -> parseFieldLength(key, config))
            .collect(Collectors.toList());
    return BlockIdLayout.from(fieldLengths.get(0), fieldLengths.get(1), fieldLengths.get(2));
  }
}
