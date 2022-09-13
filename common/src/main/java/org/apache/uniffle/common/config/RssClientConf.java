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

package org.apache.uniffle.common.config;

import java.util.List;

public class RssClientConf extends RssConf {

  public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM = ConfigOptions
      .key("rss.client.heartBeat.threadNum")
      .intType()
      .defaultValue(4)
      .withDescription("");

  public static final ConfigOption<String> RSS_CLIENT_TYPE = ConfigOptions
      .key("rss.client.type")
      .stringType()
      .defaultValue("GRPC")
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX = ConfigOptions
      .key("rss.client.retry.max")
      .intType()
      .defaultValue(100)
      .withDescription("");

  public static final ConfigOption<Long> RSS_CLIENT_RETRY_INTERVAL_MAX = ConfigOptions
      .key("rss.client.retry.interval.max")
      .longType()
      .defaultValue(10000L)
      .withDescription("");

  public static final ConfigOption<String> RSS_COORDINATOR_QUORUM = ConfigOptions
      .key("rss.coordinator.quorum")
      .stringType()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA = ConfigOptions
      .key("rss.data.replica")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE = ConfigOptions
      .key("rss.data.replica.write")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ = ConfigOptions
      .key("rss.data.replica.read")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED = ConfigOptions
      .key("rss.data.replica.skip.enabled")
      .booleanType()
      .defaultValue(true)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE = ConfigOptions
      .key("rss.client.data.transfer.pool.size")
      .intType()
      .defaultValue(Runtime.getRuntime().availableProcessors())
      .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE = ConfigOptions
      .key("rss.client.data.commit.pool.size")
      .intType()
      .defaultValue(-1)
      .withDescription("");

  public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL = ConfigOptions
      .key("rss.heartbeat.interval")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("");

  //todo
  public static final ConfigOption<Long> RSS_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.heartbeat.timeout")
      .longType()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> RSS_STORAGE_TYPE = ConfigOptions
      .key("rss.storage.type")
      .stringType()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS = ConfigOptions
      .key("rss.client.send.check.interval.ms")
      .longType()
      .defaultValue(500L)
      .withDescription("");

  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS = ConfigOptions
      .key("rss.client.send.check.timeout.ms")
      .longType()
      .defaultValue(60 * 1000 * 10L)
      .withDescription("");

  // todo
  public static final ConfigOption<Long> RSS_WRITER_BUFFER_SIZE = ConfigOptions
      .key("rss.writer.buffer.size")
      .longType()
      .noDefaultValue()
      .withDescription("Buffer size for single partition data");

  public static final ConfigOption<Integer> RSS_PARTITION_NUM_PER_RANGE = ConfigOptions
      .key("rss.partitionNum.per.range")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<String> RSS_REMOTE_STORAGE_PATH = ConfigOptions
      .key("rss.remote.storage.path")
      .stringType()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT = ConfigOptions
      .key("rss.index.read.limit")
      .intType()
      .defaultValue(500)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_NUM = ConfigOptions
      .key("rss.client.send.thread.num")
      .intType()
      .defaultValue(5)
      .withDescription("");

  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE = ConfigOptions
      .key("rss.client.read.buffer.size")
      .stringType()
      .defaultValue("14m")
      .withDescription("");

  public static final ConfigOption<List<String>> RSS_CLIENT_ASSIGNMENT_TAGS = ConfigOptions
      .key("rss.client.assignment.tags")
      .stringType()
      .asList()
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL = ConfigOptions
      .key("rss.client.assignment.retry.interval")
      .intType()
      .defaultValue(65000)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES = ConfigOptions
      .key("rss.client.assignment.retry.times")
      .intType()
      .defaultValue(3)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_ACCESS_TIMEOUT_MS = ConfigOptions
      .key("rss.access.timeout.ms")
      .intType()
      .defaultValue(10000)
      .withDescription("");

  public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED = ConfigOptions
      .key("rss.dynamicClientConf.enabled")
      .booleanType()
      .defaultValue(true)
      .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER = ConfigOptions
      .key("rss.client.assignment.shuffle.nodes.max")
      .intType()
      .defaultValue(-1)
      .withDescription("");
}
