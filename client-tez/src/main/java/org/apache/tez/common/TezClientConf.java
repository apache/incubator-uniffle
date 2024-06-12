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

package org.apache.tez.common;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;

public class TezClientConf extends RssBaseConf {
  public static final String TEZ_RSS_CONFIG_PREFIX = "tez.";

  public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM =
      ConfigOptions.key("tez.rss.client.heartBeat.threadNum")
          .intType()
          .defaultValue(4)
          .withDescription("The thread num of tez rss client heart beat thread pool.");

  public static final ConfigOption<String> RSS_CLIENT_TYPE =
      ConfigOptions.key("tez.rss.client.type")
          .stringType()
          .defaultValue("GRPC")
          .withDescription("RSS Client type, maybe GRPC");

  public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX =
      ConfigOptions.key("tez.rss.client.retry.max")
          .intType()
          .defaultValue(50)
          .withDescription("Maximum number of retries for the RSS client");

  public static final ConfigOption<Long> RSS_CLIENT_RETRY_INTERVAL_MAX =
      ConfigOptions.key("tez.rss.client.retry.interval.max")
          .longType()
          .defaultValue(10000L)
          .withDescription(
              "Maximum interval between retries for the Tez RSS client in milliseconds");

  public static final ConfigOption<String> RSS_COORDINATOR_QUORUM =
      ConfigOptions.key("tez.rss.coordinator.quorum")
          .stringType()
          .defaultValue("")
          .withDescription("Quorum configuration for the Tez RSS Client coordinator");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA =
      ConfigOptions.key("tez.rss.data.replica")
          .intType()
          .defaultValue(1)
          .withDescription("Number of data replicas for Tez RSS");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE =
      ConfigOptions.key("tez.rss.data.replica.write")
          .intType()
          .defaultValue(1)
          .withDescription("Number of write replicas for Tez RSS data");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ =
      ConfigOptions.key("tez.rss.data.replica.read")
          .intType()
          .defaultValue(1)
          .withDescription("Number of read replicas for Tez RSS data");

  public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED =
      ConfigOptions.key("tez.rss.data.replica.skip.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to enable skipping of data replicas in Tez RSS");

  public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE =
      ConfigOptions.key("tez.rss.client.data.transfer.pool.size")
          .intType()
          .defaultValue(Runtime.getRuntime().availableProcessors())
          .withDescription("Size of the data transfer pool for the Tez RSS client");

  public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE =
      ConfigOptions.key("tez.rss.client.data.commit.pool.size")
          .intType()
          .defaultValue(-1)
          .withDescription("Size of the data commit thread pool for the Tez RSS client");

  public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL =
      ConfigOptions.key("tez.rss.heartbeat.interval")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription("Interval between heartbeats for RSS in milliseconds");

  public static final ConfigOption<Long> RSS_HEARTBEAT_TIMEOUT =
      ConfigOptions.key("tez.rss.heartbeat.timeout")
          .longType()
          .defaultValue(5 * 1000L)
          .withDescription("Timeout for Tez RSS heartbeats in milliseconds");

  public static final ConfigOption<Integer> RSS_RUNTIME_IO_SORT_MB =
      ConfigOptions.key("tez.rss.runtime.io.sort.mb")
          .intType()
          .defaultValue(100)
          .withDescription("Size of the IO sort buffer for Tez RSS Client runtime in MB");

  public static final ConfigOption<Double> RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD =
      ConfigOptions.key("tez.rss.client.sort.memory.use.threshold")
          .doubleType()
          .defaultValue(0.9)
          .withDescription("Memory usage threshold for sorting in the Tez RSS client");

  public static final ConfigOption<Integer> RSS_CLIENT_MAX_BUFFER_SIZE =
      ConfigOptions.key("tez.rss.client.max.buffer.size")
          .intType()
          .defaultValue(3 * 1024)
          .withDescription("Maximum buffer size for the Tez RSS client in KB");

  public static final ConfigOption<Long> RSS_WRITER_BUFFER_SIZE =
      ConfigOptions.key("tez.rss.writer.buffer.size")
          .longType()
          .defaultValue(1024 * 1024 * 14L)
          .withDescription("Buffer size for the Tez RSS writer in bytes");

  public static final ConfigOption<Float> RSS_CLIENT_MEMORY_THRESHOLD =
      ConfigOptions.key("tez.rss.client.memory.threshold")
          .floatType()
          .defaultValue(0.8f)
          .withDescription("Memory threshold for the Tez RSS client");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_NUM =
      ConfigOptions.key("tez.rss.client.send.thread.num")
          .intType()
          .defaultValue(5)
          .withDescription("Number of send threads for the Tez RSS client");

  public static final ConfigOption<Double> RSS_CLIENT_SEND_THRESHOLD =
      ConfigOptions.key("tez.rss.client.send.threshold")
          .doubleType()
          .defaultValue(0.2)
          .withDescription("Send threshold for the Tez RSS client client");

  public static final ConfigOption<Integer> RSS_CLIENT_BATCH_TRIGGER_NUM =
      ConfigOptions.key("tez.rss.client.batch.trigger.num")
          .intType()
          .defaultValue(50)
          .withDescription("Batch trigger number for the Tez RSS client");

  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
      ConfigOptions.key("tez.rss.client.send.check.interval.ms")
          .longType()
          .defaultValue(500L)
          .withDescription("Interval in milliseconds for the Tez RSS client to check send status");

  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
      ConfigOptions.key("tez.rss.client.send.check.timeout.ms")
          .longType()
          .defaultValue(60 * 1000 * 10L)
          .withDescription("Timeout in milliseconds for the Tez RSS client send check");

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_NUM =
      ConfigOptions.key("tez.rss.client.bitmap.num")
          .intType()
          .defaultValue(1)
          .withDescription("Number of bitmaps for the Tez RSS client");

  public static final ConfigOption<String> RSS_STORAGE_TYPE =
      ConfigOptions.key("tez.rss.storage.type")
          .stringType()
          .defaultValue("MEMORY_LOCALFILE")
          .withDescription("Type of storage for the Tez RSS client");

  public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      ConfigOptions.key("tez.rss.dynamicClientConf.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether dynamic client configuration is enabled for Tez RSS Client");

  public static final ConfigOption<Boolean> RSS_AM_SLOW_START_ENABLE =
      ConfigOptions.key("tez.rss.am.slow.start.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether slow start is enabled for Tez RSS Client AM");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
      ConfigOptions.key("tez.rss.client.assignment.retry.interval")
          .intType()
          .defaultValue(65000)
          .withDescription("Interval in milliseconds for retrying client assignment in Tez RSS");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
      ConfigOptions.key("tez.rss.client.assignment.retry.times")
          .intType()
          .defaultValue(3)
          .withDescription("Number of times to retry client assignment in Tez RSS Client");

  public static final ConfigOption<String> RSS_CLIENT_ASSIGNMENT_TAGS =
      ConfigOptions.key("tez.rss.client.assignment.tags")
          .stringType()
          .defaultValue("")
          .withDescription("Tags for client assignment in Tez RSS");

  public static final ConfigOption<Boolean> RSS_AM_SHUFFLE_MANAGER_DEBUG =
      ConfigOptions.key("tez.rss.am.shuffle.manager.debug")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether debug mode is enabled for Tez RSS Client AM shuffle manager");

  public static final ConfigOption<String> RSS_AM_SHUFFLE_MANAGER_ADDRESS =
      ConfigOptions.key("tez.rss.am.shuffle.manager.address")
          .stringType()
          .defaultValue("0.0.0.0")
          .withDescription("Address for Tez RSS AM shuffle manager");

  public static final ConfigOption<Integer> RSS_AM_SHUFFLE_MANAGER_PORT =
      ConfigOptions.key("tez.rss.am.shuffle.manager.port")
          .intType()
          .defaultValue(0)
          .withDescription("Port for Tez RSS AM shuffle manager");

  public static final ConfigOption<Integer> RSS_ACCESS_TIMEOUT_MS =
      ConfigOptions.key("tez.rss.access.timeout.ms")
          .intType()
          .defaultValue(10000)
          .withDescription("Timeout in milliseconds for accessing Tez RSS Client");

  public static final ConfigOption<String> RSS_REMOTE_STORAGE_PATH =
      ConfigOptions.key("tez.rss.remote.storage.path")
          .stringType()
          .defaultValue("")
          .withDescription("Path for remote storage in Tez RSS Client");

  public static final ConfigOption<String> RSS_REMOTE_STORAGE_CONF =
      ConfigOptions.key("tez.rss.remote.storage.conf")
          .stringType()
          .defaultValue("")
          .withDescription("Configuration for remote storage in Tez RSS Client");

  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
      ConfigOptions.key("tez.rss.client.read.buffer.size")
          .stringType()
          .defaultValue("14m")
          .withDescription("Size of read buffer for Tez RSS client");

  public static final ConfigOption<Integer> RSS_PARTITION_NUM_PER_RANGE =
      ConfigOptions.key("tez.rss.partitionNum.per.range")
          .intType()
          .defaultValue(1)
          .withDescription("Number of partitions per range for Tez RSS client");

  public static final ConfigOption<Float> RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
      ConfigOptions.key("tez.rss.estimate.task.concurrency.dynamic.factor")
          .floatType()
          .defaultValue(1.0f)
          .withDescription("Dynamic factor for estimating task concurrency in Tez RSS client");

  public static final ConfigOption<Boolean> RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
      ConfigOptions.key("tez.rss.estimate.server.assignment.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether server assignment estimation is enabled in Tez RSS client");

  public static final ConfigOption<Integer> RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
      ConfigOptions.key("tez.rss.estimate.task.concurrency.per.server")
          .intType()
          .defaultValue(80)
          .withDescription("Estimated task concurrency per server in Tez RSS client");

  public static final ConfigOption<Boolean> RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK =
      ConfigOptions.key("tez.rss.avoid.recompute.succeeded.task")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to avoid recomputing succeeded tasks in Tez RSS client");

  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      ConfigOptions.key("rss.client.assignment.shuffle.nodes.max")
          .intType()
          .defaultValue(-1)
          .withDescription(
              "Maximum number of shuffle nodes for client assignment in Tez RSS client");

  public static final ConfigOption<Integer> RSS_SHUFFLE_SOURCE_VERTEX_ID =
      ConfigOptions.key("tez.rss.shuffle.source.vertex.id")
          .intType()
          .defaultValue(-1)
          .withDescription("Source vertex ID for shuffle in Tez RSS client");

  public static final ConfigOption<Integer> RSS_SHUFFLE_DESTINATION_VERTEX_ID =
      ConfigOptions.key("tez.rss.shuffle.destination.vertex.id")
          .intType()
          .defaultValue(-1)
          .withDescription("Destination vertex ID for shuffle in Tez RSS client");

  public static final ConfigOption<String> RSS_SHUFFLE_MODE =
      ConfigOptions.key("tez.shuffle.mode")
          .stringType()
          .defaultValue("remote")
          .withDescription("Mode for shuffle in Tez RSS client");

  public static final ConfigOption<String> RSS_REMOTE_SPILL_STORAGE_PATH =
      ConfigOptions.key("tez.rss.remote.spill.storage.path")
          .stringType()
          .defaultValue("")
          .withDescription("Path for remote spill storage in Tez RSS client");

  public static final ConfigOption<Boolean> RSS_REDUCE_REMOTE_SPILL_ENABLED =
      ConfigOptions.key("tez.rss.reduce.remote.spill.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether remote spill is enabled for reduce in Tez RSS client");

  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_REPLICATION =
      ConfigOptions.key("tez.rss.reduce.remote.spill.replication")
          .intType()
          .defaultValue(1)
          .withDescription("Replication for remote spill in reduce phase of Tez RSS client");

  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_RETRIES =
      ConfigOptions.key("tez.rss.reduce.remote.spill.retries")
          .intType()
          .defaultValue(5)
          .withDescription("Number of retries for remote spill in reduce phase of Tez RSS client");

  public static final ConfigOption<Boolean> TEZ_RSS_TEST_MODE_ENABLE =
      ConfigOptions.key("tez.rss.test.mode.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether enable test mode for the shuffle server.");

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(RSS_STORAGE_TYPE.key(), RSS_REMOTE_STORAGE_PATH.key());

  public Configuration getHadoopConfig() {
    return hadoopConfig;
  }

  private final Configuration hadoopConfig;

  public TezClientConf(Configuration config) {
    super();
    boolean ret = loadConfFromHadoopConfig(config);
    if (!ret) {
      throw new IllegalStateException("Fail to load config " + config);
    }
    this.hadoopConfig = config;
  }

  public boolean loadConfFromHadoopConfig(Configuration config) {
    return loadConfFromHadoopConfig(config, ConfigUtils.getAllConfigOptions(TezClientConf.class));
  }

  public RssConf toRssConf() {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, Object> entry : getAll()) {
      String key = entry.getKey();
      if (!key.startsWith(TEZ_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(TEZ_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, String.valueOf(entry.getValue()));
    }
    return rssConf;
  }

  public static RssConf toRssConf(Configuration conf) {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (!key.startsWith(TEZ_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(TEZ_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, entry.getValue());
    }
    return rssConf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TezClientConf)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TezClientConf that = (TezClientConf) o;
    return getHadoopConfig().equals(that.getHadoopConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getHadoopConfig());
  }
}
