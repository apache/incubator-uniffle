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

public class RssSparkConfig {

  public static final class SparkConfigBuilder {
    public static ConfigBuilder configBuilder;

    public static ConfigBuilder key(String key) {
      configBuilder = new ConfigBuilder(key);
      return configBuilder;
    }
  }

  public static class RssConfigEntry<T> {
    public ConfigEntry<Object> entry;
    public String key;

    public RssConfigEntry(ConfigEntry<Object> entry) {
      this.entry = entry;
      this.key = entry.key();
    }

    public T getValue() {
      return (T) entry.defaultValue().get();
    }
  }

  public static final RssConfigEntry<Integer> RSS_PARTITION_NUM_PER_RANGE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.partitionNum.per.range")
          .intConf()
          .createWithDefault(1));

  public static final RssConfigEntry<String> RSS_WRITER_BUFFER_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.buffer.size")
          .doc("controls the buffer flushing size during shuffle write")
          .stringConf()
          .createWithDefault("3m"));

  public static final RssConfigEntry<String> RSS_WRITER_SERIALIZER_BUFFER_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.serializer.buffer.size")
          .stringConf()
          .createWithDefault("3k"));

  public static final RssConfigEntry<String> RSS_WRITER_BUFFER_SEGMENT_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.buffer.segment.size")
          .stringConf()
          .createWithDefault("3k"));

  public static final RssConfigEntry<String> RSS_WRITER_BUFFER_SPILL_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.buffer.spill.size")
          .stringConf()
          .createWithDefault("128m"));

  public static final RssConfigEntry<String> RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.pre.allocated.buffer.size")
          .stringConf()
          .createWithDefault("16m"));

  public static final RssConfigEntry<Integer> RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.require.memory.retryMax")
          .intConf()
          .createWithDefault(1200));

  public static final RssConfigEntry<Long> RSS_WRITER_REQUIRE_MEMORY_INTERVAL = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.writer.require.memory.interval")
          .longConf()
          .createWithDefault(1000));

  public static final RssConfigEntry<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.send.check.timeout.ms")
          .longConf()
          .createWithDefault(60 * 1000 * 10));

  public static final RssConfigEntry<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.send.check.interval.ms")
          .longConf()
          .createWithDefault(500));

  public static final RssConfigEntry<String> RSS_TEST_FLAG = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.test")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<String> RSS_REMOTE_STORAGE_PATH = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.remote.storage.path")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<Integer> RSS_INDEX_READ_LIMIT = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.index.read.limit")
          .intConf()
          .createWithDefault(500));

  public static final RssConfigEntry<String> RSS_CLIENT_TYPE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.type")
          .stringConf()
          .createWithDefault("GRPC"));

  public static final RssConfigEntry<String> RSS_STORAGE_TYPE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.storage.type")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<Integer> RSS_CLIENT_RETRY_MAX = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.retry.max")
          .intConf()
          .createWithDefault(100));

  public static final RssConfigEntry<Long> RSS_CLIENT_RETRY_INTERVAL_MAX = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.retry.interval.max")
          .longConf()
          .createWithDefault(10000));

  public static final RssConfigEntry<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.heartBeat.threadNum")
          .intConf()
          .createWithDefault(4));

  public static final RssConfigEntry<String> RSS_CLIENT_SEND_SIZE_LIMIT = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.send.size.limit")
          .stringConf()
          .createWithDefault("16m"));

  public static final RssConfigEntry<String> RSS_CLIENT_READ_BUFFER_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.read.buffer.size")
          .stringConf()
          .createWithDefault("14m"));

  public static final RssConfigEntry<Long> RSS_HEARTBEAT_INTERVAL = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.heartbeat.interval")
          .longConf()
          .createWithDefault(10 * 1000L));

  public static final RssConfigEntry<Long> RSS_HEARTBEAT_TIMEOUT = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.heartbeat.timeout")
          .longConf()
          .createWithDefault(5 * 1000L));

  public static final RssConfigEntry<Integer> RSS_CLIENT_SEND_THREAD_POOL_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.send.threadPool.size")
          .intConf()
          .createWithDefault(10));

  public static final RssConfigEntry<Integer> RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.send.threadPool.keepalive")
          .intConf()
          .createWithDefault(60));

  public static final RssConfigEntry<Integer> RSS_DATA_REPLICA = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.data.replica")
          .intConf()
          .createWithDefault(1));

  public static final RssConfigEntry<Integer> RSS_DATA_REPLICA_WRITE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.data.replica.write")
          .intConf()
          .createWithDefault(1));

  public static final RssConfigEntry<Integer> RSS_DATA_REPLICA_READ = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.data.replica.read")
          .intConf()
          .createWithDefault(1));

  public static final RssConfigEntry<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.data.replica.skip.enabled")
          .booleanConf()
          .createWithDefault(true));

  public static final RssConfigEntry<Integer> RSS_DATA_TRANSFER_POOL_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.data.transfer.pool.size")
          .intConf()
          .createWithDefault(Runtime.getRuntime().availableProcessors()));

  public static final RssConfigEntry<Boolean> RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.ozone.dfs.namenode.odfs.enable")
          .booleanConf()
          .createWithDefault(false));

  public static final RssConfigEntry<String> RSS_OZONE_FS_HDFS_IMPL = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.ozone.fs.hdfs.impl")
          .stringConf()
          .createWithDefault("org.apache.hadoop.odfs.HdfsOdfsFilesystem"));

  public static final RssConfigEntry<String> RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.ozone.fs.AbstractFileSystem.hdfs.impl")
          .stringConf()
          .createWithDefault("org.apache.hadoop.odfs.HdfsOdfs"));

  public static final RssConfigEntry<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.bitmap.splitNum")
          .intConf()
          .createWithDefault(1));

  public static final RssConfigEntry<String> RSS_ACCESS_ID = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.access.id")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<Integer> RSS_ACCESS_TIMEOUT_MS = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.access.timeout.ms")
          .intConf()
          .createWithDefault(10000));

  public static final RssConfigEntry<Boolean> RSS_ENABLED = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.enabled")
          .booleanConf()
          .createWithDefault(false));

  public static final RssConfigEntry<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.dynamicClientConf.enabled")
          .booleanConf()
          .createWithDefault(true));

  public static final RssConfigEntry<String> RSS_CLIENT_ASSIGNMENT_TAGS = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.assignment.tags")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<String> RSS_COORDINATOR_QUORUM = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.coordinator.quorum")
          .stringConf()
          .createWithDefault(""));

  public static final RssConfigEntry<Integer> RSS_DATA_COMMIT_POOL_SIZE = new RssConfigEntry(
      SparkConfigBuilder
          .key("spark.rss.client.data.commit.pool.size")
          .intConf()
          .createWithDefault(-1));

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      Sets.newHashSet(RSS_STORAGE_TYPE.key, RSS_REMOTE_STORAGE_PATH.key);

  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";

  public static final boolean RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE = false;

}
