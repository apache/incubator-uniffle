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

import com.tencent.rss.client.util.RssClientConfig;

public class RssSparkConfig {

  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";
  public static final String RSS_PARTITION_NUM_PER_RANGE =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_PARTITION_NUM_PER_RANGE;
  public static final int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE =
      RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE;
  public static final String RSS_WRITER_BUFFER_SIZE =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_WRITER_BUFFER_SIZE;
  public static final String RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = "3m";
  public static final String RSS_WRITER_SERIALIZER_BUFFER_SIZE =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.serializer.buffer.size";
  public static final String RSS_WRITER_SERIALIZER_BUFFER_SIZE_DEFAULT_VALUE = "3k";
  public static final String RSS_WRITER_BUFFER_SEGMENT_SIZE =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.buffer.segment.size";
  public static final String RSS_WRITER_BUFFER_SEGMENT_SIZE_DEFAULT_VALUE = "3k";
  public static final String RSS_WRITER_BUFFER_SPILL_SIZE =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.buffer.spill.size";
  public static final String RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE = "128m";
  public static final String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.pre.allocated.buffer.size";
  public static final String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE_DEFAULT_VALUE = "16m";
  public static final String RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.require.memory.retryMax";
  public static final int RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX_DEFAULT_VALUE = 1200;
  public static final String RSS_WRITER_REQUIRE_MEMORY_INTERVAL =
      SPARK_RSS_CONFIG_PREFIX + "rss.writer.require.memory.interval";
  public static final long RSS_WRITER_REQUIRE_MEMORY_INTERVAL_DEFAULT_VALUE = 1000; // 1s
  public static final String RSS_COORDINATOR_QUORUM =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM;
  public static final String RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS;
  public static final long RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE; // 10 min
  public static final String RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS;
  public static final long RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE;
  public static final String RSS_TEST_FLAG = SPARK_RSS_CONFIG_PREFIX + "rss.test";
  public static final String RSS_REMOTE_STORAGE_PATH =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_STORAGE_PATH;
  public static final String RSS_INDEX_READ_LIMIT =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_INDEX_READ_LIMIT;
  public static final int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE = 500;
  public static final String RSS_CLIENT_TYPE =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_TYPE;
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE = RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE;
  public static final String RSS_STORAGE_TYPE = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE;
  public static final String RSS_CLIENT_RETRY_MAX = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX;
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE;
  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX;
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE;
  public static final String RSS_CLIENT_HEARTBEAT_THREAD_NUM =
      SPARK_RSS_CONFIG_PREFIX + "rss.client.heartBeat.threadNum";
  public static final int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;
  public static final String RSS_CLIENT_SEND_SIZE_LIMIT = SPARK_RSS_CONFIG_PREFIX + "rss.client.send.size.limit";
  public static final String RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE = "16m";
  public static final String RSS_CLIENT_READ_BUFFER_SIZE =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE;
  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static final String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE;
  public static final String RSS_HEARTBEAT_INTERVAL = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_INTERVAL;
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE = RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE;
  public static final String RSS_HEARTBEAT_TIMEOUT = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_TIMEOUT;
  public static final String RSS_CLIENT_SEND_THREAD_POOL_SIZE =
      SPARK_RSS_CONFIG_PREFIX + "rss.client.send.threadPool.size";
  public static final int RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE = 10;
  public static final String RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE =
      SPARK_RSS_CONFIG_PREFIX + "rss.client.send.threadPool.keepalive";
  public static final int RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE = 60;
  public static final String RSS_DATA_REPLICA = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA;
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE = RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_WRITE = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_WRITE;
  public static final int RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE = RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_READ = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_READ;
  public static final int RSS_DATA_REPLICA_READ_DEFAULT_VALUE = RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_SKIP_ENABLED =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED;
  public static final String RSS_DATA_TRANSFER_POOL_SIZE =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE;
  public static final int RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE;
  public static final boolean RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE;
  public static final String RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE =
      SPARK_RSS_CONFIG_PREFIX + "rss.ozone.dfs.namenode.odfs.enable";
  public static final boolean RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE_DEFAULT_VALUE = false;
  public static final String RSS_OZONE_FS_HDFS_IMPL = SPARK_RSS_CONFIG_PREFIX + "rss.ozone.fs.hdfs.impl";
  public static final String RSS_OZONE_FS_HDFS_IMPL_DEFAULT_VALUE = "org.apache.hadoop.odfs.HdfsOdfsFilesystem";
  public static final String RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL =
      SPARK_RSS_CONFIG_PREFIX + "rss.ozone.fs.AbstractFileSystem.hdfs.impl";
  public static final String RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL_DEFAULT_VALUE =
      "org.apache.hadoop.odfs.HdfsOdfs";
  // todo: remove unnecessary configuration
  public static final String RSS_CLIENT_BITMAP_SPLIT_NUM =
      SPARK_RSS_CONFIG_PREFIX + "rss.client.bitmap.splitNum";
  public static final int RSS_CLIENT_BITMAP_SPLIT_NUM_DEFAULT_VALUE = 1;
  public static final String RSS_ACCESS_ID = SPARK_RSS_CONFIG_PREFIX + "rss.access.id";
  public static final String RSS_ACCESS_TIMEOUT_MS = SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ACCESS_TIMEOUT_MS;
  public static final int RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE = RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE;
  public static final String RSS_ENABLED = SPARK_RSS_CONFIG_PREFIX + "rss.enabled";
  public static final boolean RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE = false;
  public static final String RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED;
  public static final boolean RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE =
      RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE;

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      Sets.newHashSet(RSS_STORAGE_TYPE, RSS_REMOTE_STORAGE_PATH);
}
