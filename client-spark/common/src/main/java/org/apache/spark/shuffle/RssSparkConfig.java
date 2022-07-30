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

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.TypedConfigBuilder;

import scala.Serializable;
import scala.runtime.AbstractFunction1;

import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.client.util.RssClientConfig;

public class RssSparkConfig {

  public static final ConfigEntry<Integer> RSS_PARTITION_NUM_PER_RANGE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.partitionNum.per.range")
          .doc("xxxxxx"))
      .createWithDefault(10);

  public static final ConfigEntry<String> RSS_WRITER_BUFFER_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.writer.buffer.size")
          .doc("controls the buffer flushing size during shuffle write"))
      .createWithDefault("3m");

  public static final ConfigEntry<String> RSS_WRITER_SERIALIZER_BUFFER_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.writer.serializer.buffer.size")
          .doc(""))
      .createWithDefault("3k");

  public static final ConfigEntry<String> RSS_WRITER_BUFFER_SEGMENT_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.writer.buffer.segment.size")
          .doc(""))
      .createWithDefault("3k");

  public static final ConfigEntry<String> RSS_WRITER_BUFFER_SPILL_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.writer.buffer.spill.size")
          .doc(""))
      .createWithDefault("128m");

  public static final ConfigEntry<String> RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.writer.pre.allocated.buffer.size")
          .doc(""))
      .createWithDefault("16m");

  public static final ConfigEntry<Integer> RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX = createIntegerBuilder(
      new ConfigBuilder("spark.rss.writer.require.memory.retryMax")
          .doc(""))
      .createWithDefault(1200);

  public static final ConfigEntry<Long> RSS_WRITER_REQUIRE_MEMORY_INTERVAL = createLongBuilder(
      new ConfigBuilder("spark.rss.writer.require.memory.interval")
          .doc(""))
      .createWithDefault(1000L);

  public static final ConfigEntry<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS = createLongBuilder(
      new ConfigBuilder("spark.rss.client.send.check.timeout.ms")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);

  public static final ConfigEntry<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS = createLongBuilder(
      new ConfigBuilder("spark.rss.client.send.check.interval.ms")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE);

  public static final ConfigEntry<String> RSS_TEST_FLAG = createStringBuilder(
      new ConfigBuilder("spark.rss.test")
          .doc(""))
      .createWithDefault("");

  public static final ConfigEntry<String> RSS_REMOTE_STORAGE_PATH = createStringBuilder(
      new ConfigBuilder("spark.rss.remote.storage.path")
          .doc(""))
      .createWithDefault("");

  public static final ConfigEntry<Integer> RSS_INDEX_READ_LIMIT = createIntegerBuilder(
      new ConfigBuilder("spark.rss.index.read.limit")
          .doc(""))
      .createWithDefault(500);

  public static final ConfigEntry<String> RSS_CLIENT_TYPE = createStringBuilder(
      new ConfigBuilder("spark.rss.client.type")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);

  public static final ConfigEntry<String> RSS_STORAGE_TYPE = createStringBuilder(
      new ConfigBuilder("spark.rss.storage.type")
          .doc(""))
      .createWithDefault("");

  public static final ConfigEntry<Integer> RSS_CLIENT_RETRY_MAX = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.retry.max")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);

  public static final ConfigEntry<Long> RSS_CLIENT_RETRY_INTERVAL_MAX = createLongBuilder(
      new ConfigBuilder("spark.rss.client.retry.interval.max")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.heartBeat.threadNum")
          .doc(""))
      .createWithDefault(4);

  public static final ConfigEntry<String> RSS_CLIENT_SEND_SIZE_LIMIT = createStringBuilder(
      new ConfigBuilder("spark.rss.client.send.size.limit")
          .doc(""))
      .createWithDefault("16m");

  public static final ConfigEntry<String> RSS_CLIENT_READ_BUFFER_SIZE = createStringBuilder(
      new ConfigBuilder("spark.rss.client.read.buffer.size")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);

  public static final ConfigEntry<Long> RSS_HEARTBEAT_INTERVAL = createLongBuilder(
      new ConfigBuilder("spark.rss.heartbeat.interval")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);

  public static final ConfigEntry<Long> RSS_HEARTBEAT_TIMEOUT = createLongBuilder(
      new ConfigBuilder("spark.rss.heartbeat.timeout")
          .doc(""))
      .createWithDefault(5 * 1000L);

  public static final ConfigEntry<Integer> RSS_CLIENT_SEND_THREAD_POOL_SIZE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.send.threadPool.size")
          .doc(""))
      .createWithDefault(10);

  public static final ConfigEntry<Integer> RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.send.threadPool.keepalive")
          .doc(""))
      .createWithDefault(60);

  public static final ConfigEntry<Integer> RSS_DATA_REPLICA = createIntegerBuilder(
      new ConfigBuilder("spark.rss.data.replica")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_DATA_REPLICA_WRITE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.data.replica.write")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_DATA_REPLICA_READ = createIntegerBuilder(
      new ConfigBuilder("spark.rss.data.replica.read")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);

  public static final ConfigEntry<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED = createBooleanBuilder(
      new ConfigBuilder("spark.rss.data.replica.skip.enabled")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_DATA_TRANSFER_POOL_SIZE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.data.transfer.pool.size")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_DATA_COMMIT_POOL_SIZE = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.data.commit.pool.size")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);

  public static final ConfigEntry<Boolean> RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE = createBooleanBuilder(
      new ConfigBuilder("spark.rss.ozone.dfs.namenode.odfs.enable")
          .doc(""))
      .createWithDefault(false);

  public static final ConfigEntry<String> RSS_OZONE_FS_HDFS_IMPL = createStringBuilder(
      new ConfigBuilder("spark.rss.ozone.fs.hdfs.impl")
          .doc(""))
      .createWithDefault("org.apache.hadoop.odfs.HdfsOdfsFilesystem");

  public static final ConfigEntry<String> RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL = createStringBuilder(
      new ConfigBuilder("spark.rss.ozone.fs.AbstractFileSystem.hdfs.impl")
          .doc(""))
      .createWithDefault("org.apache.hadoop.odfs.HdfsOdfs");

  public static final ConfigEntry<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM = createIntegerBuilder(
      new ConfigBuilder("spark.rss.client.bitmap.splitNum")
          .doc(""))
      .createWithDefault(1);

  public static final ConfigEntry<String> RSS_ACCESS_ID = createStringBuilder(
      new ConfigBuilder("spark.rss.access.id")
          .doc(""))
      .createWithDefault("");

  public static final ConfigEntry<Integer> RSS_ACCESS_TIMEOUT_MS = createIntegerBuilder(
      new ConfigBuilder("spark.rss.access.timeout.ms")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE);

  public static final ConfigEntry<Boolean> RSS_ENABLED = createBooleanBuilder(
      new ConfigBuilder("spark.rss.enabled")
          .doc(""))
      .createWithDefault(false);

  public static final ConfigEntry<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED = createBooleanBuilder(
      new ConfigBuilder("spark.rss.dynamicClientConf.enabled")
          .doc(""))
      .createWithDefault(RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

  public static final ConfigEntry<String> RSS_CLIENT_ASSIGNMENT_TAGS = createStringBuilder(
      new ConfigBuilder("spark.rss.client.assignment.tags")
          .doc(""))
      .createWithDefault("");

  public static final ConfigEntry<String> RSS_COORDINATOR_QUORUM = createStringBuilder(
      new ConfigBuilder("spark.rss.coordinator.quorum")
          .doc(""))
      .createWithDefault("");

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      Sets.newHashSet(RSS_STORAGE_TYPE.key(), RSS_REMOTE_STORAGE_PATH.key());

  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";

  public static final boolean RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE = false;

  public static TypedConfigBuilder<Integer> createIntegerBuilder(ConfigBuilder builder) {
    scala.Function1<String, Integer> f = new SerializableFunction1<String, Integer>() {
      @Override
      public Integer apply(String in) {
        return ConfigUtils.convertValue(in, Integer.class);
      }
    };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Long> createLongBuilder(ConfigBuilder builder) {
    scala.Function1<String, Long> f = new SerializableFunction1<String, Long>() {
      @Override
      public Long apply(String in) {
        return ConfigUtils.convertValue(in, Long.class);
      }
    };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Boolean> createBooleanBuilder(ConfigBuilder builder) {
    scala.Function1<String, Boolean> f = new SerializableFunction1<String, Boolean>() {
      @Override
      public Boolean apply(String in) {
        return ConfigUtils.convertValue(in, Boolean.class);
      }
    };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Double> createDoubleBuilder(ConfigBuilder builder) {
    scala.Function1<String, Double> f = new SerializableFunction1<String, Double>() {
      @Override
      public Double apply(String in) {
        return ConfigUtils.convertValue(in, Double.class);
      }
    };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<String> createStringBuilder(ConfigBuilder builder) {
    return builder.stringConf();
  }
}

abstract class SerializableFunction1<T1, R> extends AbstractFunction1<T1, R> implements Serializable {
}
