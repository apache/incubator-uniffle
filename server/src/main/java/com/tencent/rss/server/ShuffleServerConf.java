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

package com.tencent.rss.server;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.ConfigUtils;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.RssUtils;

public class ShuffleServerConf extends RssBaseConf {

  public static final String PREFIX_HADOOP_CONF = "rss.server.hadoop";

  public static final ConfigOption<Long> SERVER_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.buffer.capacity")
      .longType()
      .noDefaultValue()
      .withDescription("Max memory of buffer manager for shuffle server");

  public static final ConfigOption<Long> SERVER_READ_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.read.buffer.capacity")
      .longType()
      .defaultValue(10000L)
      .withDescription("Max size of buffer for reading data");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_DELAY = ConfigOptions
      .key("rss.server.heartbeat.delay")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat initial delay ms");

  public static final ConfigOption<Integer> SERVER_HEARTBEAT_THREAD_NUM = ConfigOptions
      .key("rss.server.heartbeat.threadNum")
      .intType()
      .defaultValue(2)
      .withDescription("rss heartbeat thread number");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_INTERVAL = ConfigOptions
      .key("rss.server.heartbeat.interval")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("Heartbeat interval to Coordinator (ms)");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.server.heartbeat.timeout")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("rss heartbeat interval ms");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.size")
      .intType()
      .defaultValue(10)
      .withDescription("thread pool for flush data to file");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.queue.size")
      .intType()
      .defaultValue(Integer.MAX_VALUE)
      .withDescription("size of waiting queue for thread pool");

  public static final ConfigOption<Long> SERVER_FLUSH_THREAD_ALIVE = ConfigOptions
      .key("rss.server.flush.thread.alive")
      .longType()
      .defaultValue(120L)
      .withDescription("thread idle time in pool (s)");

  public static final ConfigOption<Long> SERVER_COMMIT_TIMEOUT = ConfigOptions
      .key("rss.server.commit.timeout")
      .longType()
      .defaultValue(600000L)
      .withDescription("Timeout when commit shuffle data (ms)");

  public static final ConfigOption<Integer> SERVER_WRITE_RETRY_MAX = ConfigOptions
      .key("rss.server.write.retry.max")
      .intType()
      .defaultValue(10)
      .withDescription("Retry times when write fail");

  public static final ConfigOption<Long> SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT = ConfigOptions
      .key("rss.server.app.expired.withoutHeartbeat")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Expired time (ms) for application which has no heartbeat with coordinator");

  public static final ConfigOption<Integer> SERVER_MEMORY_REQUEST_RETRY_MAX = ConfigOptions
      .key("rss.server.memory.request.retry.max")
      .intType()
      .defaultValue(100)
      .withDescription("Max times to retry for memory request");

  public static final ConfigOption<Long> SERVER_PRE_ALLOCATION_EXPIRED = ConfigOptions
      .key("rss.server.preAllocation.expired")
      .longType()
      .defaultValue(20 * 1000L)
      .withDescription("Expired time (ms) for pre allocated buffer");

  public static final ConfigOption<Long> SERVER_COMMIT_CHECK_INTERVAL_MAX = ConfigOptions
      .key("rss.server.commit.check.interval.max.ms")
      .longType()
      .defaultValue(10000L)
      .withDescription("Max interval(ms) for check commit status");

  public static final ConfigOption<Long> SERVER_WRITE_SLOW_THRESHOLD = ConfigOptions
      .key("rss.server.write.slow.threshold")
      .longType()
      .defaultValue(10000L)
      .withDescription("Threshold for write slow defined");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L1 = ConfigOptions
      .key("rss.server.event.size.threshold.l1")
      .longType()
      .defaultValue(200000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L2 = ConfigOptions
      .key("rss.server.event.size.threshold.l2")
      .longType()
      .defaultValue(1000000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L3 = ConfigOptions
      .key("rss.server.event.size.threshold.l3")
      .longType()
      .defaultValue(10000000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Double> CLEANUP_THRESHOLD = ConfigOptions
      .key("rss.server.cleanup.threshold")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator, "clean threshold must be between 0.0 and 100.0")
      .defaultValue(10.0)
      .withDescription("Threshold for disk cleanup");

  public static final ConfigOption<Double> HIGH_WATER_MARK_OF_WRITE = ConfigOptions
      .key("rss.server.high.watermark.write")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator, "high write watermark must be between 0.0 and 100.0")
      .defaultValue(95.0)
      .withDescription("If disk usage is bigger than this value, disk cannot been written");

  public static final ConfigOption<Double> LOW_WATER_MARK_OF_WRITE = ConfigOptions
      .key("rss.server.low.watermark.write")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator, "low write watermark must be between 0.0 and 100.0")
      .defaultValue(85.0)
      .withDescription("If disk usage is smaller than this value, disk can been written again");

  public static final ConfigOption<Long> PENDING_EVENT_TIMEOUT_SEC = ConfigOptions
      .key("rss.server.pending.event.timeout.sec")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "pending event timeout must be positive")
      .defaultValue(600L)
      .withDescription("If disk cannot be written for timeout seconds, the flush data event will fail");

  public static final ConfigOption<Boolean> UPLOADER_ENABLE = ConfigOptions
      .key("rss.server.uploader.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("A switch of the uploader");

  public static final ConfigOption<Integer> UPLOADER_THREAD_NUM = ConfigOptions
      .key("rss.server.uploader.thread.number")
      .intType()
      .checkValue(ConfigUtils.positiveIntegerValidator2, "uploader thread number must be positive")
      .defaultValue(4)
      .withDescription("The thread number of the uploader");

  public static final ConfigOption<Long> UPLOADER_INTERVAL_MS = ConfigOptions
      .key("rss.server.uploader.interval.ms")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "uploader interval must be positive")
      .defaultValue(3000L)
      .withDescription("The interval for the uploader");

  public static final ConfigOption<Long> UPLOAD_COMBINE_THRESHOLD_MB = ConfigOptions
      .key("rss.server.uploader.combine.threshold.mb")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "uploader combine threshold must be positive")
      .defaultValue(32L)
      .withDescription("The threshold of the combine mode");

  public static final ConfigOption<String> UPLOADER_BASE_PATH = ConfigOptions
      .key("rss.server.uploader.base.path")
      .stringType()
      .noDefaultValue()
      .withDescription("The base path of the uploader");

  public static final ConfigOption<String> UPLOAD_STORAGE_TYPE = ConfigOptions
      .key("rss.server.uploader.remote.storage.type")
      .stringType()
      .defaultValue("HDFS")
      .withDescription("The remote storage type of the uploader");

  public static final ConfigOption<Long> REFERENCE_UPLOAD_SPEED_MBS = ConfigOptions
      .key("rss.server.uploader.references.speed.mbps")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "uploader reference speed must be positive")
      .defaultValue(8L)
      .withDescription("The speed for the uploader");

  public static final ConfigOption<Long> DISK_CAPACITY = ConfigOptions
      .key("rss.server.disk.capacity")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "disk capacity must be positive")
      .defaultValue(1024L * 1024L * 1024L * 1024L)
      .withDescription("Disk capacity that shuffle server can use");

  public static final ConfigOption<Long> CLEANUP_INTERVAL_MS = ConfigOptions
      .key("rss.server.cleanup.interval.ms")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "cleanup interval must be positive")
      .defaultValue(3000L)
      .withDescription("The interval for cleanup");

  public static final ConfigOption<Long> SHUFFLE_EXPIRED_TIMEOUT_MS = ConfigOptions
      .key("rss.server.shuffle.expired.timeout.ms")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "shuffle expired timeout must be positive")
      .defaultValue(60L * 1000 * 2)
      .withDescription("If the shuffle is not read for the long time, and shuffle is uploaded totally,"
          + " , we can delete the shuffle");

  public static final ConfigOption<Long> SHUFFLE_MAX_UPLOAD_SIZE = ConfigOptions
      .key("rss.server.shuffle.max.upload.size")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "max upload size must be positive")
      .defaultValue(1024L * 1024L * 1024L)
      .withDescription("The max value of upload shuffle size");

  public static final ConfigOption<Double> SHUFFLE_MAX_FORCE_UPLOAD_TIME_RATIO = ConfigOptions
      .key("rss.server.shuffle.max.force.upload.time.ratio")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator, "max upload time radio must between 0.0 and 100.0")
      .defaultValue(95.0)
      .withDescription("The max upload time ratio in force mode");

  public static final ConfigOption<Long> SERVER_SHUFFLE_INDEX_SIZE_HINT = ConfigOptions
      .key("rss.server.index.size.hint")
      .longType()
      .defaultValue(2 * 1024L * 1024L)
      .withDescription("The index file size hint");

  public static final ConfigOption<Double> HEALTH_STORAGE_MAX_USAGE_PERCENTAGE = ConfigOptions
      .key("rss.server.health.max.storage.usage.percentage")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator,
          "The max usage percentage must be between 0.0 and 100.0")
      .defaultValue(90.0)
      .withDescription("The usage percentage of a storage exceed the value, the disk become unavailable");

  public static final ConfigOption<Double> HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE = ConfigOptions
      .key("rss.server.health.storage.recovery.usage.percentage")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator,
          "The recovery usage percentage must be between 0.0 and 100.0")
      .defaultValue(80.0)
      .withDescription("The usage percentage of an unavailable storage decline the value, the disk"
          + " will become available");

  public static final ConfigOption<Long> HEALTH_CHECK_INTERVAL = ConfigOptions
      .key("rss.server.health.check.interval.ms")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator,  "The interval for health check must be positive")
      .defaultValue(5000L)
      .withDescription("The interval for health check");

  public static final ConfigOption<Double> HEALTH_MIN_STORAGE_PERCENTAGE = ConfigOptions
      .key("rss.server.health.min.storage.percentage")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator,
          "The minimum for healthy storage percentage must be between 0.0 and 100.0")
      .defaultValue(80.0)
      .withDescription("The minimum fraction of storage that must pass the check mark the node as healthy");

  public static final ConfigOption<Boolean> HEALTH_CHECK_ENABLE = ConfigOptions
      .key("rss.server.health.check.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("The switch for the health check");

  public static final ConfigOption<String> HEALTH_CHECKER_CLASS_NAMES = ConfigOptions
      .key("rss.server.health.checker.class.names")
      .stringType()
      .noDefaultValue()
      .withDescription("The list of the Checker's name");

  public static final ConfigOption<Double> SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE = ConfigOptions
      .key("rss.server.memory.shuffle.lowWaterMark.percentage")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator,
          "The lowWaterMark for memory percentage must be between 0.0 and 100.0")
      .defaultValue(25.0)
      .withDescription("LowWaterMark of memory in percentage style");

  public static final ConfigOption<Double> SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE = ConfigOptions
      .key("rss.server.memory.shuffle.highWaterMark.percentage")
      .doubleType()
      .checkValue(ConfigUtils.percentageDoubleValidator,
          "The highWaterMark for memory percentage must be between 0.0 and 100.0")
      .defaultValue(75.0)
      .withDescription("HighWaterMark of memory in percentage style");

  public static final ConfigOption<Long> FLUSH_COLD_STORAGE_THRESHOLD_SIZE = ConfigOptions
      .key("rss.server.flush.cold.storage.threshold.size")
      .longType()
      .checkValue(ConfigUtils.positiveLongValidator, "flush cold storage threshold must be positive")
      .defaultValue(64L * 1024L * 1024L)
      .withDescription("For multistorage, the event size exceed this value, flush data  to cold storage");

  public static final ConfigOption<Long> FALLBACK_MAX_FAIL_TIMES = ConfigOptions
      .key("rss.server.multistorage.fallback.max.fail.times")
      .longType()
      .checkValue(ConfigUtils.non_negativeLongValidator, " fallback times must be non-negative")
      .defaultValue(0L)
      .withDescription("For multistorage, fail times exceed the number, will switch storage");

  public ShuffleServerConf() {
  }

  public ShuffleServerConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public Configuration getHadoopConf() {
    Configuration hadoopConf = new Configuration();
    for (String key : getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        hadoopConf.set(hadoopKey, value);
      }
    }
    return hadoopConf;
  }

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);

    if (properties == null) {
      return false;
    }

    loadCommonConf(properties);

    List<ConfigOption> configOptions = ConfigUtils.getAllConfigOptions(ShuffleServerConf.class);

    properties.forEach((k, v) -> {
      configOptions.forEach(config -> {
        if (config.key().equalsIgnoreCase(k)) {
          set(config, ConfigUtils.convertValue(v, config.getClazz()));
        }
      });

      if (k.startsWith(PREFIX_HADOOP_CONF)) {
        setString(k, v);
      }
    });

    return true;
  }
}
