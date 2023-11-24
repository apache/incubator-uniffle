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

package org.apache.hadoop.mapreduce;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigOption;


public class RssMRConfig {

  public static final String MR_CONFIG_PREFIX = "mapreduce.";

  public static final String MR_RSS_CONFIG_PREFIX = "mapreduce.rss.";

  public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.heartBeat.threadNum")
          .intType()
          .defaultValue(4)
          .withDescription("");


  public static final ConfigOption<String> RSS_CLIENT_TYPE =
      ConfigOptions.key("mapreduce.rss.client.type")
          .stringType()
          .defaultValue(RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE)
          .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Long> RSS_CLIENT_RETRY_INTERVAL_MAX =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE)
          .withDescription("");


  public static final String RSS_COORDINATOR_QUORUM = MR_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM;

  public static final ConfigOption<Integer> RSS_DATA_REPLICA =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE)
          .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_WRITE)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_READ)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED =
     ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED)
          .booleanType()
          .defaultValue(RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE)
            .intType()
            .defaultValue(RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE)
            .withDescription("");


  public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE)
          .intType()
          .defaultValue(RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_NUM =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_THREAD_NUM)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_DEFAULT_SEND_NUM)
          .withDescription("");


  public static final ConfigOption<Double> RSS_CLIENT_SEND_THRESHOLD =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.send.threshold")
              .doubleType()
              .defaultValue(0.2f)
              .withDescription("");


  public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_INTERVAL)
          .longType()
          .defaultValue(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE)
          .withDescription("");


  public static final String RSS_HEARTBEAT_TIMEOUT =MR_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_TIMEOUT;


  public static final String RSS_ASSIGNMENT_PREFIX = MR_CONFIG_PREFIX + "rss.assignment.partition.";


  public static final ConfigOption<Integer> RSS_CLIENT_BATCH_TRIGGER_NUM =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.batch.trigger.num")
          .intType()
          .defaultValue(50)
          .withDescription("");


  public static final ConfigOption<Double> RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.sort.memory.use.threshold")
          .doubleType()
          .defaultValue(0.9f)
          .withDescription("");


  public static final ConfigOption<Long> RSS_WRITER_BUFFER_SIZE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_WRITER_BUFFER_SIZE)
          .longType()
          .defaultValue(1024 * 1024 * 14)
          .withDescription("");


  public static final ConfigOption<Double> RSS_CLIENT_MEMORY_THRESHOLD =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.memory.threshold")
          .doubleType()
          .defaultValue(0.8f)
          .withDescription("");


  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE)
          .withDescription("");



  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_NUM =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.bitmap.num")
          .intType()
          .defaultValue(1)
          .withDescription("");


  public static final ConfigOption<Long> RSS_CLIENT_MAX_SEGMENT_SIZE =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.client.max.buffer.size")
          .longType()
          .defaultValue(3 * 1024)
          .withDescription("");


  public static final String RSS_STORAGE_TYPE = MR_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE;



  public static final ConfigOption<Boolean> RSS_REDUCE_REMOTE_SPILL_ENABLED =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.reduce.remote.spill.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.reduce.remote.spill.attempt.inc")
          .intType()
          .defaultValue(1)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_REPLICATION =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.reduce.remote.spill.replication")
          .intType()
          .defaultValue(1)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_RETRIES =
      ConfigOptions.key(MR_CONFIG_PREFIX + "rss.reduce.remote.spill.retries")
          .intType()
          .defaultValue(5)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_PARTITION_NUM_PER_RANGE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_PARTITION_NUM_PER_RANGE)
          .intType()
          .defaultValue(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE)
          .withDescription("");



  public static final String RSS_REMOTE_STORAGE_PATH =
      MR_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_STORAGE_PATH;


  public static final String RSS_REMOTE_STORAGE_CONF = MR_CONFIG_PREFIX + "rss.remote.storage.conf";

  public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_INDEX_READ_LIMIT)
          .intType()
          .defaultValue(RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE)
          .withDescription("");



  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE)
          .stringType()
          .defaultValue(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED)
          .booleanType()
          .defaultValue(RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_ACCESS_TIMEOUT_MS =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_ACCESS_TIMEOUT_MS)
          .intType()
          .defaultValue(RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE)
          .withDescription("");

  public static final String RSS_CLIENT_ASSIGNMENT_TAGS =
      MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_TAGS;

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      ConfigOptions.key(RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE)
          .withDescription("");



  public static final ConfigOption<Long> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL)
          .longType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES)
          .intType()
          .defaultValue(RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Boolean> RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED)
          .booleanType()
          .defaultValue(RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Double> RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR)
          .doubleType()
          .defaultValue(RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE)
          .withDescription("");


  public static final ConfigOption<Integer> RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
      ConfigOptions.key(MR_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER)
          .intType()
          .defaultValue(RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE)
          .withDescription("");


  public static final String RSS_CONF_FILE = "rss_conf.xml";

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(RSS_STORAGE_TYPE, RSS_REMOTE_STORAGE_PATH);

  // Whether enable test mode for the MR Client
  public static final String RSS_TEST_MODE_ENABLE =
      MR_CONFIG_PREFIX + RssClientConfig.RSS_TEST_MODE_ENABLE;

  public static RssConf toRssConf(Configuration jobConf) {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, String> entry : jobConf) {
      String key = entry.getKey();
      if (!key.startsWith(MR_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(MR_CONFIG_PREFIX.length());
      rssConf.setString(key, entry.getValue());
    }
    return rssConf;
  }
}
