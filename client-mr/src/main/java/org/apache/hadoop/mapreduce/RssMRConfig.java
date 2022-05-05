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

package org.apache.hadoop.mapreduce;

public class RssMRConfig {

  public static final String RSS_CLIENT_HEARTBEAT_THREAD_NUM = "mapreduce.rss.client.heartBeat.threadNum";
  public static final int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;
  public static final String RSS_CLIENT_TYPE = "mapreduce.rss.client.type";
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE = "GRPC";
  public static final String RSS_CLIENT_RETRY_MAX = "mapreduce.rss.client.retry.max";
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = 100;
  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX = "mapreduce.rss.client.retry.interval.max";
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE = 10000;
  public static final String RSS_COORDINATOR_QUORUM = "mapreduce.rss.coordinator.quorum";
  public static final String RSS_DATA_REPLICA = "mapreduce.rss.data.replica";
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE = 1;
  public static final String RSS_DATA_REPLICA_WRITE = "mapreduce.rss.data.replica.write";
  public static final int RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE = 1;
  public static final String RSS_DATA_REPLICA_READ = "mapreduce.rss.data.replica.read";
  public static final int RSS_DATA_REPLICA_READ_DEFAULT_VALUE = 1;
  public static final String RSS_HEARTBEAT_INTERVAL = "mapreduce.rss.heartbeat.interval";
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE = 10 * 1000L;
  public static final String RSS_HEARTBEAT_TIMEOUT = "mapreduce.rss.heartbeat.timeout";
  public static final String RSS_ASSIGNMENT_PREFIX = "mapreduce.rss.assignment.partition.";
  public static final String RSS_CLIENT_BATCH_TRIGGER_NUM = "mapreduce.rss.client.batch.trigger.num";
  public static final int RSS_CLIENT_DEFAULT_BATCH_TRIGGER_NUM = 50;
  public static final String RSS_CLIENT_MEMORY_THRESHOLD = "mapreduce.rss.client.memory.threshold";
  public static final double RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD = 0.8f;
  public static final String RSS_CLIENT_SEND_CHECK_INTERVAL_MS = "mapreduce.rss.client.send.check.interval.ms";
  public static final long RSS_CLIENT_DEFAULT_SEND_CHECK_INTERVAL_MS = 500;
  public static final String RSS_CLIENT_SEND_CHECK_TIMEOUT_MS = "mapreduce.rss.client.send.check.timeout.ms";
  public static final long RSS_CLIENT_DEFAULT_CHECK_TIMEOUT_MS = 60 * 1000 * 10;
  public static final String RSS_CLIENT_BITMAP_NUM = "mapreduce.rss.client.bitmap.num";
  public static final int RSS_CLIENT_DEFAULT_BITMAP_NUM = 1;
  public static final String RSS_CLIENT_MAX_SEGMENT_SIZE = "mapreduce.rss.client.max.buffer.size";
  public static final long RSS_CLIENT_DEFAULT_MAX_SEGMENT_SIZE = 3 * 1024;
  public static final String RSS_STORAGE_TYPE = "mapreduce.rss.storage.type";

  public static final String RSS_PARTITION_NUM_PER_RANGE = "mapreduce.rss.partitionNum.per.range";
  public static final int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE = 1;
  public static final String RSS_BASE_PATH = "mapreduce.rss.remote.storage.path";
  public static final String RSS_INDEX_READ_LIMIT = "mapreduce.rss.index.read.limit";
  public static final int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE = 500;
  public static String RSS_CLIENT_READ_BUFFER_SIZE = "mapreduce.rss.client.read.buffer.size";
  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE = "14m";

  public static String RSS_DYNAMIC_CLIENT_CONF_ENABLED = "mapreduce.rss.dynamicClientConf.enabled";
  public static boolean RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE = true;
}
