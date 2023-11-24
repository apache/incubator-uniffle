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

import org.apache.spark.internal.config.ConfigEntry;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.internal.config.TypedConfigBuilder;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssConf;

public class RssSparkConfig {

  public static final ConfigOption<Long> RSS_CLIENT_SEND_SIZE_LIMITATION =
          ConfigOptions.key("rss.client.send.size.limit")
                  .longType()
                  .defaultValue(1024 * 1024 * 16L)
                  .withDescription("The max data size sent to shuffle server");

  public static final ConfigOption<Integer> RSS_MEMORY_SPILL_TIMEOUT =
          ConfigOptions.key("rss.client.memory.spill.timeout.sec")
                  .intType()
                  .defaultValue(1)
                  .withDescription(
                          "The timeout of spilling data to remote shuffle server, "
                                  + "which will be triggered by Spark TaskMemoryManager. Unit is sec, default value is 1");

  public static final ConfigOption<Boolean> RSS_ROW_BASED =
          ConfigOptions.key("rss.row.based")
                  .booleanType()
                  .defaultValue(true)
                  .withDescription("indicates row based shuffle, set false when use in columnar shuffle");

  public static final ConfigOption<Boolean> RSS_MEMORY_SPILL_ENABLED =
          ConfigOptions.key("rss.client.memory.spill.enabled")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription(
                          "The memory spill switch triggered by Spark TaskMemoryManager, default value is false.");

  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";

  public static final ConfigOption<Integer> RSS_PARTITION_NUM_PER_RANGE =
          ConfigOptions.key("spark.rss.partitionNum.per.range")
                  .intType()
                  .defaultValue(1)
                  .withDescription("The partition number of one range.");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SIZE =
          ConfigOptions.key("spark.rss.writer.buffer.size")
                  .stringType()
                  .defaultValue("3m")
                  .withDescription("Buffer size for single partition data");


  public static final ConfigOption<String> RSS_WRITER_SERIALIZER_BUFFER_SIZE =
          ConfigOptions.key("spark.rss.writer.serializer.buffer.size")
                  .stringType()
                  .defaultValue("3k")
                  .withDescription("");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SEGMENT_SIZE =
          ConfigOptions.key("spark.rss.writer.buffer.segment.size")
                  .stringType()
                  .defaultValue("3k")
                  .withDescription("");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SPILL_SIZE =
          ConfigOptions.key("spark.rss.writer.buffer.spill.size")
                  .stringType()
                  .defaultValue("128m")
                  .withDescription("Buffer size for total partition data");


  public static final ConfigOption<String> RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE =
          ConfigOptions.key("spark.rss.writer.pre.allocated.buffer.size")
                  .stringType()
                  .defaultValue("16m")
                  .withDescription("");

  public static final ConfigEntry<Integer> RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX =
          createIntegerBuilder(new ConfigBuilder("spark.rss.writer.require.memory.retryMax"))
                  .createWithDefault(1200);

  public static final ConfigEntry<Long> RSS_WRITER_REQUIRE_MEMORY_INTERVAL =
          createLongBuilder(new ConfigBuilder("spark.rss.writer.require.memory.interval"))
                  .createWithDefault(1000L);

  public static final ConfigEntry<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
          createLongBuilder(
                  new ConfigBuilder(
                          SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS))
                  .createWithDefault(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);


  public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
          ConfigOptions.key("spark.rss.client.send.check.interval.ms")
                  .longType()
                  .defaultValue(500L)
                  .withDescription("");

  public static final ConfigOption<Boolean> RSS_TEST_FLAG =
          ConfigOptions.key("spark.rss.test")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("");

  public static final ConfigEntry<Boolean> RSS_TEST_MODE_ENABLE =
          createBooleanBuilder(
                  new ConfigBuilder(SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_TEST_MODE_ENABLE)
                          .doc("Whether enable test mode for the Spark Client"))
                  .createWithDefault(false);


  public static final ConfigOption<String> RSS_REMOTE_STORAGE_PATH =
          ConfigOptions.key("spark.rss.remote.storage.path")
                  .stringType()
                  .defaultValue("")
                  .withDescription("");

  public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT =
          ConfigOptions.key("spark.rss.index.read.limit")
                  .intType()
                  .defaultValue(500)
                  .withDescription("");

  public static final ConfigEntry<String> RSS_CLIENT_TYPE =
          createStringBuilder(
                  new ConfigBuilder(SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_TYPE))
                  .createWithDefault(RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);

  public static final ConfigEntry<String> RSS_STORAGE_TYPE =
          createStringBuilder(
                  new ConfigBuilder(SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE)
                          .doc("Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS"))
                  .createWithDefault("");

  public static final ConfigEntry<Integer> RSS_CLIENT_RETRY_MAX =
          createIntegerBuilder(
                  new ConfigBuilder(SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX))
                  .createWithDefault(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);

  public static final ConfigEntry<Long> RSS_CLIENT_RETRY_INTERVAL_MAX =
          createLongBuilder(
                  new ConfigBuilder(
                          SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX))
                  .createWithDefault(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM =
          ConfigOptions.key("spark.rss.client.heartBeat.threadNum")
                  .intType()
                  .defaultValue(4)
                  .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE =
          ConfigOptions.key("spark.rss.client.unregister.thread.pool.size")
                  .intType()
                  .defaultValue(10)
                  .withDescription("");

  public static final ConfigOption<Integer> RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC =
          ConfigOptions.key("spark.rss.client.unregister.request.timeout.sec")
                  .intType()
                  .defaultValue(10)
                  .withDescription("");

  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
          ConfigOptions.key("spark.rss.client.read.buffer.size")
                  .stringType()
                  .defaultValue("14m")
                  .withDescription("The max data size read from storage");


  public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL =
          ConfigOptions.key("spark.rss.heartbeat.interval")
                  .longType()
                  .defaultValue(10 * 1000L)
                  .withDescription("The max data size read from storage");

  public static final ConfigOption<Long> RSS_HEARTBEAT_TIMEOUT =
          ConfigOptions.key("spark.rss.heartbeat.timeout")
                  .longType()
                  .defaultValue(5 * 1000L)
                  .withDescription("");


  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_SIZE =
          ConfigOptions.key("spark.rss.client.send.threadPool.size")
                  .intType()
                  .defaultValue(10)
                  .withDescription("The thread size for send shuffle data to shuffle server");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE =
          ConfigOptions.key("spark.rss.client.send.threadPool.keepalive")
                  .intType()
                  .defaultValue(60)
                  .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA =
          ConfigOptions.key("spark.rss.rss.data.replica")
                  .intType()
                  .defaultValue(1)
                  .withDescription("The max server number that each block can be send by client in quorum protocol");

  public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE =
          ConfigOptions.key("spark.rss.data.replica.write")
                  .intType()
                  .defaultValue(1)
                  .withDescription("The min server number that each block should be send by client successfully");



  public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ =
          ConfigOptions.key("spark.rss.data.replica.read")
                  .intType()
                  .defaultValue(1)
                  .withDescription("The min server number that metadata should be fetched by client successfully");


  public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED =
          ConfigOptions.key("spark.rss.data.replica.skip.enabled")
                  .booleanType()
                  .defaultValue(true)
                  .withDescription("");


  public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE =
          ConfigOptions.key("spark.rss.client.data.transfer.pool.size")
                  .intType()
                  .defaultValue(RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE)
                  .withDescription("");

  public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE =
          ConfigOptions.key("spark.rss.client.data.commit.pool.size")
                  .intType()
                  .defaultValue(-1)
                  .withDescription("The thread size for sending commit to shuffle servers");


  public static final ConfigEntry<Boolean> RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE =
          createBooleanBuilder(new ConfigBuilder("spark.rss.ozone.dfs.namenode.odfs.enable"))
                  .createWithDefault(false);


  public static final ConfigEntry<String> RSS_OZONE_FS_HDFS_IMPL =
          createStringBuilder(new ConfigBuilder("spark.rss.ozone.fs.hdfs.impl"))
                  .createWithDefault("org.apache.hadoop.odfs.HdfsOdfsFilesystem");

  public static final ConfigEntry<String> RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL =
          createStringBuilder(new ConfigBuilder("spark.rss.ozone.fs.AbstractFileSystem.hdfs.impl"))
                  .createWithDefault("org.apache.hadoop.odfs.HdfsOdfs");

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM =
          ConfigOptions.key("spark.rss.client.bitmap.splitNum")
                  .intType()
                  .defaultValue(1)
                  .withDescription("");

  public static final ConfigOption<String> RSS_ACCESS_ID =
          ConfigOptions.key("spark.rss.access.id")
                  .stringType()
                  .defaultValue("")
                  .withDescription("");


  public static final ConfigOption<Integer> RSS_ACCESS_TIMEOUT_MS =
          ConfigOptions.key("spark.rss.access.timeout.ms")
                  .intType()
                  .defaultValue(10000)
                  .withDescription("");

  public static final ConfigOption<Boolean> RSS_ENABLED =
          ConfigOptions.key("spark.rss.enabled")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("");

  public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED =
          ConfigOptions.key("spark.rss.dynamicClientConf.enabled")
                  .booleanType()
                  .defaultValue(true)
                  .withDescription("");

  public static final ConfigOption<String> RSS_CLIENT_ASSIGNMENT_TAGS =
          ConfigOptions.key("spark.rss.client.assignment.tags")
                  .stringType()
                  .defaultValue("")
                  .withDescription("The comma-separated list of tags for deciding assignment shuffle servers. "
                          + "Notice that the SHUFFLE_SERVER_VERSION will always as the assignment tag "
                          + "whether this conf is set or not");


  public static final ConfigOption<Boolean> RSS_CLIENT_OFF_HEAP_MEMORY_ENABLE =
          ConfigOptions.key("spark.rss.client.off.heap.memory.enable")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("Client can use off heap memory");

  public static final ConfigEntry<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      createIntegerBuilder(
              new ConfigBuilder(
                  SPARK_RSS_CONFIG_PREFIX
                      + RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER))
          .createWithDefault(
              RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE);


  public static final ConfigOption<Long> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
          ConfigOptions.key("spark.rss.client.assignment.retry.interval")
                  .longType()
                  .defaultValue(65000l)
                  .withDescription("");


  public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
          ConfigOptions.key("spark.rss.client.assignment.retry.times")
                  .intType()
                  .defaultValue(3)
                  .withDescription("");

  public static final ConfigOption<Long> RSS_CLIENT_ACCESS_RETRY_INTERVAL_MS =
          ConfigOptions.key("spark.rss.client.access.retry.interval.ms")
                  .longType()
                  .defaultValue(20000L)
                  .withDescription("Interval between retries fallback to SortShuffleManager");

  public static final ConfigOption<Integer> RSS_CLIENT_ACCESS_RETRY_TIMES =
          ConfigOptions.key("spark.rss.client.access.retry.times")
                  .intType()
                  .defaultValue(0)
                  .withDescription("Number of retries fallback to SortShuffleManager");

  public static final ConfigEntry<String> RSS_COORDINATOR_QUORUM =
      createStringBuilder(
              new ConfigBuilder(SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM)
                  .doc("Coordinator quorum"))
          .createWithDefault("");

  public static final ConfigEntry<Double> RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
      createDoubleBuilder(
              new ConfigBuilder(
                      SPARK_RSS_CONFIG_PREFIX
                          + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR)
                  .doc(
                      "Between 0 and 1, used to estimate task concurrency, how likely is this part of the resource between"
                          + " spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors"
                          + " to be allocated"))
          .createWithDefault(
              RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE);

  public static final ConfigEntry<Boolean> RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
      createBooleanBuilder(
              new ConfigBuilder(
                      SPARK_RSS_CONFIG_PREFIX
                          + RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED)
                  .doc(
                      "Whether to estimate the number of ShuffleServers to be allocated based on the number"
                          + " of concurrent tasks."))
          .createWithDefault(RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE);

  public static final ConfigEntry<Integer> RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
      createIntegerBuilder(
              new ConfigBuilder(
                      SPARK_RSS_CONFIG_PREFIX
                          + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER)
                  .doc(
                      "How many tasks concurrency to allocate a ShuffleServer, you need to enable"
                          + " spark.rss.estimate.server.assignment.enabled"))
          .createWithDefault(
              RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE);

  public static final ConfigOption<Integer> RSS_SHUFFLE_MANAGER_GRPC_PORT =
          ConfigOptions.key("spark.rss.shuffle.manager.grpc.port")
                  .intType()
                  .noDefaultValue()
                  .withDescription("internal configuration to indicate which port is actually bind for shuffle manager service.");

  public static final ConfigOption<Boolean> RSS_RESUBMIT_STAGE =
          ConfigOptions.key("spark.rss.resubmit.stage")
                  .booleanType()
                  .defaultValue(false)
                  .withDescription("Whether to enable the resubmit stage.");

  // spark2 doesn't have this key defined
  public static final String SPARK_SHUFFLE_COMPRESS_KEY = "spark.shuffle.compress";

  public static final boolean SPARK_SHUFFLE_COMPRESS_DEFAULT = true;

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(RSS_STORAGE_TYPE.key(), RSS_REMOTE_STORAGE_PATH.key());

  public static final boolean RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE = false;

  public static TypedConfigBuilder<Integer> createIntegerBuilder(ConfigBuilder builder) {
    scala.Function1<String, Integer> f =
        new AbstractFunction1<String, Integer>() {
          @Override
          public Integer apply(String in) {
            return ConfigUtils.convertValue(in, Integer.class);
          }
        };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Long> createLongBuilder(ConfigBuilder builder) {
    scala.Function1<String, Long> f =
        new AbstractFunction1<String, Long>() {
          @Override
          public Long apply(String in) {
            return ConfigUtils.convertValue(in, Long.class);
          }
        };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Boolean> createBooleanBuilder(ConfigBuilder builder) {
    scala.Function1<String, Boolean> f =
        new AbstractFunction1<String, Boolean>() {
          @Override
          public Boolean apply(String in) {
            return ConfigUtils.convertValue(in, Boolean.class);
          }
        };
    return new TypedConfigBuilder<>(builder, f);
  }

  public static TypedConfigBuilder<Double> createDoubleBuilder(ConfigBuilder builder) {
    scala.Function1<String, Double> f =
        new AbstractFunction1<String, Double>() {
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

  public static RssConf toRssConf(SparkConf sparkConf) {
    RssConf rssConf = new RssConf();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      String key = tuple._1;
      if (!key.startsWith(SPARK_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(SPARK_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, tuple._2);
    }
    return rssConf;
  }
}