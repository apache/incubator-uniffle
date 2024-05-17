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

package org.apache.uniffle.flink.config;

import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.RssConf;

public class RssFlinkConfig {
  public static final String FLINK_RSS_CONFIG_PREFIX = "flink.";

  public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM =
      ConfigOptions.key("flink.rss.client.heartBeat.threadNum").intType().defaultValue(4);

  public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);

  public static final ConfigOption<Long> RSS_CLIENT_RETRY_INTERVAL_MAX =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);

  public static final ConfigOption<String> RSS_CLIENT_TYPE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_TYPE)
          .stringType()
          .defaultValue(RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_WRITE)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_READ)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_DATA_REPLICA =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);

  public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED)
          .booleanType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);

  public static final ConfigOption<String> RSS_COORDINATOR_QUORM =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM)
          .stringType()
          .defaultValue("");

  public static final ConfigOption<String> RSS_QUOTA_USER =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + "rss.quota.user")
          .stringType()
          .defaultValue("user");

  public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_INTERVAL)
          .longType()
          .defaultValue(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);

  public static final ConfigOption<Long> RSS_HEARTBEAT_TIMEOUT =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_TIMEOUT)
          .longType()
          .defaultValue(5 * 1000L);

  public static final ConfigOption<Long> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
      ConfigOptions.key(
              FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

  public static final ConfigOption<String> RSS_STORAGE_TYPE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE)
          .stringType()
          .defaultValue("");

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + "rss.client.bitmap.splitNum")
          .intType()
          .defaultValue(1);

  public static final ConfigOption<String> RSS_REMOTE_STORAGE_PATH =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_STORAGE_PATH)
          .stringType()
          .defaultValue("");

  public static final ConfigOption<String> RSS_CLIENT_ASSIGNMENT_TAGS =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_TAGS)
          .stringType()
          .defaultValue("");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      ConfigOptions.key(
              FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE);

  public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED)
          .booleanType()
          .defaultValue(RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

  public static final ConfigOption<MemorySize> RSS_MEMORY_PER_RESULT_PARTITION =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + "remote-shuffle.job.memory-per-partition")
          .memoryType()
          .defaultValue(MemorySize.parse("64m"));

  public static final ConfigOption<MemorySize> RSS_MEMORY_PER_INPUT_GATE =
      ConfigOptions.key(FLINK_RSS_CONFIG_PREFIX + "remote-shuffle.job.memory-per-gate")
          .memoryType()
          .defaultValue(MemorySize.parse("32m"));

  public static RssConf toRssConf(Configuration flinkConf) {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, String> entry : flinkConf.toMap().entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(FLINK_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(FLINK_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, entry.getValue());
    }
    return rssConf;
  }
}
