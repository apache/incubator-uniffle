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
}
