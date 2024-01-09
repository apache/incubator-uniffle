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

package org.apache.hadoop.mapreduce;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.JobConf;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;

public class MRClientConf extends RssBaseConf {
    public static final String MR_CONFIG_PREFIX = "mapreduce.";
    public static final String MR_RSS_CONFIG_PREFIX = "mapreduce.rss.";
    public static final String RSS_ASSIGNMENT_PREFIX = "mapreduce.rss.assignment.partition.";
    public static final String RSS_CONF_FILE = "rss_conf.xml";

    public static final ConfigOption<Integer> RSS_CLIENT_HEARTBEAT_THREAD_NUM =
            ConfigOptions.key("mapreduce.rss.client.heartBeat.threadNum")
                    .intType()
                    .defaultValue(4)
                    .withDescription("");

    public static final ConfigOption<String> RSS_CLIENT_TYPE =
            ConfigOptions.key("mapreduce.rss.client.type")
                    .stringType()
                    .defaultValue("GRPC")
                    .withDescription("RSS Client type, maybe GRPC");

    public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX =
            ConfigOptions.key("mapreduce.rss.client.retry.max").intType().defaultValue(50).withDescription("");

    public static final ConfigOption<Long> RSS_CLIENT_RETRY_INTERVAL_MAX =
            ConfigOptions.key("mapreduce.rss.client.retry.interval.max")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("");

    public static final ConfigOption<String> RSS_COORDINATOR_QUORUM =
            ConfigOptions.key("mapreduce.rss.coordinator.quorum")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_DATA_REPLICA =
            ConfigOptions.key("mapreduce.rss.data.replica").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Integer> RSS_DATA_REPLICA_WRITE =
            ConfigOptions.key("mapreduce.rss.data.replica.write").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Integer> RSS_DATA_REPLICA_READ =
            ConfigOptions.key("mapreduce.rss.data.replica.read").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Boolean> RSS_DATA_REPLICA_SKIP_ENABLED =
            ConfigOptions.key("mapreduce.rss.data.replica.skip.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_DATA_TRANSFER_POOL_SIZE =
            ConfigOptions.key("mapreduce.rss.client.data.transfer.pool.size")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_DATA_COMMIT_POOL_SIZE =
            ConfigOptions.key("mapreduce.rss.client.data.commit.pool.size")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_NUM =
            ConfigOptions.key("mapreduce.rss.client.send.thread.num")
                    .intType()
                    .defaultValue(5)
                    .withDescription("");

    public static final ConfigOption<Double> RSS_CLIENT_SEND_THRESHOLD =
            ConfigOptions.key("mapreduce.rss.client.send.threshold")
                    .doubleType()
                    .defaultValue(0.2)
                    .withDescription("");

    public static final ConfigOption<Long> RSS_HEARTBEAT_INTERVAL =
            ConfigOptions.key("mapreduce.rss.heartbeat.interval")
                    .longType()
                    .defaultValue(10 * 1000L)
                    .withDescription("");

    public static final ConfigOption<Long> RSS_HEARTBEAT_TIMEOUT =
            ConfigOptions.key("mapreduce.rss.heartbeat.timeout")
                    .longType()
                    .defaultValue(5 * 1000L)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_BATCH_TRIGGER_NUM =
            ConfigOptions.key("mapreduce.rss.client.batch.trigger.num")
                    .intType()
                    .defaultValue(50)
                    .withDescription("");

    public static final ConfigOption<Double> RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD =
            ConfigOptions.key("mapreduce.rss.client.sort.memory.use.threshold")
                    .doubleType()
                    .defaultValue(0.9)
                    .withDescription("");

    public static final ConfigOption<Long> RSS_WRITER_BUFFER_SIZE =
            ConfigOptions.key("mapreduce.rss.writer.buffer.size")
                    .longType()
                    .defaultValue(1024 * 1024 * 14L)
                    .withDescription("");

    public static final ConfigOption<Float> RSS_CLIENT_MEMORY_THRESHOLD =
            ConfigOptions.key("mapreduce.rss.client.memory.threshold")
                    .floatType()
                    .defaultValue(0.8f)
                    .withDescription("");

    public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
            ConfigOptions.key("mapreduce.rss.client.send.check.interval.ms")
                    .longType()
                    .defaultValue(500L)
                    .withDescription("");

    public static final ConfigOption<Long> RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
            ConfigOptions.key("mapreduce.rss.client.send.check.timeout.ms")
                    .longType()
                    .defaultValue(60 * 1000 * 10L)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_NUM =
            ConfigOptions.key("mapreduce.rss.client.bitmap.num").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_MAX_BUFFER_SIZE =
            ConfigOptions.key("mapreduce.rss.client.max.buffer.size")
                    .intType()
                    .defaultValue(3 * 1024)
                    .withDescription("");

    public static final ConfigOption<String> RSS_STORAGE_TYPE =
            ConfigOptions.key("mapreduce.rss.storage.type")
                    .stringType()
                    .defaultValue("MEMORY_LOCALFILE")
                    .withDescription("");

    public static final ConfigOption<Boolean> RSS_REDUCE_REMOTE_SPILL_ENABLED =
            ConfigOptions.key("mapreduce.rss.reduce.remote.spill.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC =
            ConfigOptions.key("mapreduce.rss.reduce.remote.spill.attempt.inc")
                    .intType()
                    .defaultValue(1)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_REPLICATION =
            ConfigOptions.key("mapreduce.rss.reduce.remote.spill.replication")
                    .intType()
                    .defaultValue(1)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_RETRIES =
            ConfigOptions.key("mapreduce.rss.reduce.remote.spill.retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_PARTITION_NUM_PER_RANGE =
            ConfigOptions.key("mapreduce.rss.partitionNum.per.range")
                    .intType()
                    .defaultValue(1)
                    .withDescription("");

    public static final ConfigOption<String> RSS_REMOTE_STORAGE_PATH =
            ConfigOptions.key("mapreduce.rss.remote.storage.path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<String> RSS_REMOTE_STORAGE_CONF =
            ConfigOptions.key("mapreduce.rss.remote.storage.conf")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT =
            ConfigOptions.key("mapreduce.rss.index.read.limit")
                    .intType()
                    .defaultValue(500)
                    .withDescription("");

    public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
            ConfigOptions.key("mapreduce.rss.client.read.buffer.size")
                    .stringType()
                    .defaultValue("14m")
                    .withDescription("When the size of read buffer reaches the half of JVM region (i.e., 32m),it will incur humongous allocation, so we set it to 14m. ");

    public static final ConfigOption<Boolean> RSS_DYNAMIC_CLIENT_CONF_ENABLED =
            ConfigOptions.key("mapreduce.rss.dynamicClientConf.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_ACCESS_TIMEOUT_MS =
            ConfigOptions.key("mapreduce.rss.access.timeout.ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("");

    public static final ConfigOption<String> RSS_CLIENT_ASSIGNMENT_TAGS =
            ConfigOptions.key("mapreduce.rss.client.assignment.tags")
                    .stringType()
                    .defaultValue("")
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
            ConfigOptions.key("rss.client.assignment.shuffle.nodes.max")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
            ConfigOptions.key("mapreduce.rss.client.assignment.retry.interval")
                    .intType()
                    .defaultValue(65000)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
            ConfigOptions.key("mapreduce.rss.client.assignment.retry.times")
                    .intType()
                    .defaultValue(3)
                    .withDescription("");

    public static final ConfigOption<Boolean> RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
            ConfigOptions.key("mapreduce.rss.estimate.server.assignment.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("");

    public static final ConfigOption<Float> RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
            ConfigOptions.key("mapreduce.rss.estimate.task.concurrency.dynamic.factor")
                    .floatType()
                    .defaultValue(1.0f)
                    .withDescription("");

    public static final ConfigOption<Integer> RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
            ConfigOptions.key("mapreduce.rss.estimate.task.concurrency.per.server")
                    .intType()
                    .defaultValue(80)
                    .withDescription("");

    public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
            ImmutableSet.of(RSS_STORAGE_TYPE.key(), RSS_REMOTE_STORAGE_PATH.key());

    public static final ConfigOption<Boolean> MR_RSS_TEST_MODE_ENABLE =
            ConfigOptions.key("mapreduce.rss.test.mode.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable test mode for the shuffle server.");

    public JobConf getHadoopConfig() {
        return hadoopConfig;
    }

    private final JobConf hadoopConfig;

    public MRClientConf() {
        this(new JobConf());
    }

    public MRClientConf(JobConf config) {
        super();
        boolean ret = loadConfFromHadoopConfig(config);
        if (!ret) {
            throw new IllegalStateException("Fail to load config " + config);
        }
        this.hadoopConfig = config;
    }

    public boolean loadConfFromHadoopConfig(JobConf config) {
        return loadConfFromHadoopConfig(config, ConfigUtils.getAllConfigOptions(MRClientConf.class));
    }

    public static RssConf toRssConf(JobConf jobConf) {
        RssConf rssConf = new RssConf();
        for (Map.Entry<String, String> entry : jobConf) {
            String key = entry.getKey();
            if (!key.startsWith(MR_CONFIG_PREFIX)) {
                continue;
            }
            key = key.substring(MR_CONFIG_PREFIX.length());
            rssConf.setString(key, entry.getValue());
        }
        return rssConf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MRClientConf)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MRClientConf that = (MRClientConf) o;
        return getHadoopConfig().equals(that.getHadoopConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getHadoopConfig());
    }
}