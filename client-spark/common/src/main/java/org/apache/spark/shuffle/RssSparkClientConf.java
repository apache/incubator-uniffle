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

package org.apache.spark.shuffle;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

public class RssSparkClientConf extends RssClientConf {
  public static final String SPARK_CONFIG_KEY_PREFIX = "spark.";
  public static final String SPARK_CONFIG_RSS_KEY_PREFIX = SPARK_CONFIG_KEY_PREFIX + "rss.";

  public static final String DEFAULT_RSS_WRITER_BUFFER_SIZE = "3m";
  public static final long DEFAULT_RSS_HEARTBEAT_TIMEOUT = 5 * 1000L;

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(RSS_STORAGE_TYPE.key(), RSS_REMOTE_STORAGE_PATH.key());

  public static final ConfigOption<String> RSS_WRITER_SERIALIZER_BUFFER_SIZE = ConfigOptions
      .key("rss.writer.serializer.buffer.size")
      .stringType()
      .defaultValue("3k")
      .withDescription("");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SEGMENT_SIZE = ConfigOptions
      .key("rss.writer.buffer.segment.size")
      .stringType()
      .defaultValue("3k")
      .withDescription("");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SPILL_SIZE = ConfigOptions
      .key("rss.writer.buffer.spill.size")
      .stringType()
      .defaultValue("128m")
      .withDescription("Buffer size for total partition data");

  public static final ConfigOption<String> RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE = ConfigOptions
      .key("rss.writer.pre.allocated.buffer.size")
      .stringType()
      .defaultValue("16m")
      .withDescription("Buffer size for total partition data");

  public static final ConfigOption<Integer> RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX = ConfigOptions
      .key("rss.writer.require.memory.retryMax")
      .intType()
      .defaultValue(1200)
      .withDescription("");

  public static final ConfigOption<Long> RSS_WRITER_REQUIRE_MEMORY_INTERVAL = ConfigOptions
      .key("rss.writer.require.memory.interval")
      .longType()
      .defaultValue(1000L)
      .withDescription("");

  public static final ConfigOption<Boolean> RSS_TEST_FLAG = ConfigOptions
      .key("rss.test")
      .booleanType()
      .defaultValue(false);

  public static final ConfigOption<String> RSS_CLIENT_SEND_SIZE_LIMIT = ConfigOptions
      .key("rss.client.send.size.limit")
      .stringType()
      .defaultValue("16m")
      .withDescription("The max data size sent to shuffle server");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_SIZE  = ConfigOptions
      .key("rss.client.send.threadPool.size")
      .intType()
      .defaultValue(10)
      .withDescription("The thread size for send shuffle data to shuffle server");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE  = ConfigOptions
      .key("rss.client.send.threadPool.keepalive")
      .intType()
      .defaultValue(60)
      .withDescription("");

  public static final ConfigOption<Boolean> RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE  = ConfigOptions
      .key("rss.ozone.dfs.namenode.odfs.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("");

  public static final ConfigOption<String> RSS_OZONE_FS_HDFS_IMPL  = ConfigOptions
      .key("rss.ozone.fs.hdfs.impl")
      .stringType()
      .defaultValue("org.apache.hadoop.odfs.HdfsOdfsFilesystem")
      .withDescription("");

  public static final ConfigOption<String> RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL  = ConfigOptions
      .key("rss.ozone.fs.AbstractFileSystem.hdfs.impl")
      .stringType()
      .defaultValue("org.apache.hadoop.odfs.HdfsOdfs")
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM  = ConfigOptions
      .key("rss.client.bitmap.splitNum")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<String> RSS_ACCESS_ID  = ConfigOptions
      .key("rss.access.id")
      .stringType()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Boolean> RSS_ENABLED  = ConfigOptions
      .key("rss.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("");

  public static final ConfigOption<Long> RSS_CLIENT_ACCESS_RETRY_INTERVAL_MS  = ConfigOptions
      .key("rss.client.access.retry.interval.ms")
      .longType()
      .defaultValue(20000L)
      .withDescription("Interval between retries fallback to SortShuffleManager");

  public static final ConfigOption<Integer> RSS_CLIENT_ACCESS_RETRY_TIMES  = ConfigOptions
      .key("rss.client.access.retry.times")
      .intType()
      .defaultValue(0)
      .withDescription("Number of retries fallback to SortShuffleManager");

  public RssSparkClientConf() {
    // ignore
  }

  private RssSparkClientConf(SparkConf sparkConf) {
    List<ConfigOption<Object>> configOptions = ConfigUtils.getAllConfigOptions(RssSparkClientConf.class);

    Map<String, ConfigOption<Object>> configOptionMap = configOptions
        .stream()
        .collect(
            Collectors.toMap(
                entry -> entry.key(),
                entry -> entry
            )
        );

    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      String key = tuple._1;
      if (!key.startsWith(SPARK_CONFIG_RSS_KEY_PREFIX)) {
        continue;
      }
      key = key.substring(SPARK_CONFIG_KEY_PREFIX.length());
      String val = tuple._2;
      ConfigOption configOption = configOptionMap.get(key);
      if (configOption != null) {
        set(configOption, ConfigUtils.convertValue(val, configOption.getClazz()));
      }
    }
  }

  public static RssSparkClientConf from(SparkConf sparkConf) {
    return new RssSparkClientConf(sparkConf);
  }

  public static String toKey(ConfigOption option) {
    return String.format("%s%s", SPARK_CONFIG_KEY_PREFIX, option.key());
  }

  @VisibleForTesting
  public static void toSparkConf(RssConf rssConf, SparkConf sparkConf) {
    List<Pair<String, Object>> confs = rssConf.getAll();
    for (Pair<String, Object> conf : confs) {
      sparkConf.set(SPARK_CONFIG_KEY_PREFIX + conf.getLeft(), String.valueOf(conf.getRight()));
    }
    return;
  }
}
