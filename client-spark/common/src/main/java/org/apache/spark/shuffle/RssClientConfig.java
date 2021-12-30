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

package org.apache.spark.shuffle;

public class RssClientConfig {

  public static String RSS_PARTITION_NUM_PER_RANGE = "spark.rss.partitionNum.per.range";
  public static int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE = 1;
  public static String RSS_WRITER_BUFFER_SIZE = "spark.rss.writer.buffer.size";
  public static String RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = "3m";
  public static String RSS_WRITER_SERIALIZER_BUFFER_SIZE = "spark.rss.writer.serializer.buffer.size";
  public static String RSS_WRITER_SERIALIZER_BUFFER_SIZE_DEFAULT_VALUE = "3k";
  public static String RSS_WRITER_BUFFER_SEGMENT_SIZE = "spark.rss.writer.buffer.segment.size";
  public static String RSS_WRITER_BUFFER_SEGMENT_SIZE_DEFAULT_VALUE = "3k";
  public static String RSS_WRITER_BUFFER_SPILL_SIZE = "spark.rss.writer.buffer.spill.size";
  public static String RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE = "128m";
  public static String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE = "spark.rss.writer.pre.allocated.buffer.size";
  public static String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE_DEFAULT_VALUE = "16m";
  public static String RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX = "spark.rss.writer.require.memory.retryMax";
  public static int RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX_DEFAULT_VALUE = 600;
  public static String RSS_WRITER_REQUIRE_MEMORY_INTERVAL = "spark.rss.writer.require.memory.interval";
  public static long RSS_WRITER_REQUIRE_MEMORY_INTERVAL_DEFAULT_VALUE = 1000; // 1s
  public static String RSS_COORDINATOR_QUORUM = "spark.rss.coordinator.quorum";
  public static String RSS_WRITER_SEND_CHECK_TIMEOUT = "spark.rss.writer.send.check.timeout";
  public static long RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE = 5 * 60 * 1000; // 5 min
  public static String RSS_WRITER_SEND_CHECK_INTERVAL = "spark.rss.writer.send.check.interval";
  public static long RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE = 500;
  public static String RSS_TEST_FLAG = "spark.rss.test";
  public static String RSS_BASE_PATH = "spark.rss.base.path";
  public static String RSS_INDEX_READ_LIMIT = "spark.rss.index.read.limit";
  public static int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE = 500;
  public static String RSS_CLIENT_TYPE = "spark.rss.client.type";
  public static String RSS_CLIENT_TYPE_DEFAULT_VALUE = "GRPC";
  public static String RSS_STORAGE_TYPE = "spark.rss.storage.type";
  public static String RSS_CLIENT_RETRY_MAX = "spark.rss.client.retry.max";
  public static int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = 50;
  public static String RSS_CLIENT_RETRY_INTERVAL_MAX = "spark.rss.client.retry.interval.max";
  public static long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE = 10000;
  public static String RSS_CLIENT_HEARTBEAT_THREAD_NUM = "spark.rss.client.heartBeat.threadNum";
  public static int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;
  public static String RSS_CLIENT_SEND_SIZE_LIMIT = "spark.rss.client.send.size.limit";
  public static String RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE = "16m";
  public static String RSS_CLIENT_READ_BUFFER_SIZE = "spark.rss.client.read.buffer.size";
  public static String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE = "32m";
  public static String RSS_HEARTBEAT_INTERVAL = "spark.rss.heartbeat.interval";
  public static long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE = 10 * 1000L;
  public static String RSS_HEARTBEAT_TIMEOUT = "spark.rss.heartbeat.timeout";
  public static String RSS_CLIENT_SEND_THREAD_POOL_SIZE = "spark.rss.client.send.threadPool.size";
  public static int RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE = 10;
  public static String RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE = "spark.rss.client.send.threadPool.keepalive";
  public static int RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE = 60;
  public static String RSS_DATA_REPLICA = "spark.rss.data.replica";
  public static int RSS_DATA_REPLICA_DEFAULT_VALUE = 1;
  public static String RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE = "spark.rss.ozone.dfs.namenode.odfs.enable";
  public static boolean RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE_DEFAULT_VALUE = false;
  public static String RSS_OZONE_FS_HDFS_IMPL = "spark.rss.ozone.fs.hdfs.impl";
  public static String RSS_OZONE_FS_HDFS_IMPL_DEFAULT_VALUE = "org.apache.hadoop.odfs.HdfsOdfsFilesystem";
  public static String RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL = "spark.rss.ozone.fs.AbstractFileSystem.hdfs.impl";
  public static String RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL_DEFAULT_VALUE = "org.apache.hadoop.odfs.HdfsOdfs";
  // it is used in shuffle server to decide how many bitmap should be used to store blockId
  // the target for this is to improve shuffle server's performance of report/get shuffle result
  // currently, all blockIds are stored in multiple shuffle servers, update this config if there
  // has huge amount blockIds, eg, 10B.
  public static String RSS_CLIENT_BITMAP_SPLIT_NUM = "spark.rss.client.bitmap.splitNum";
  public static int RSS_CLIENT_BITMAP_SPLIT_NUM_DEFAULT_VALUE = 1;
}
