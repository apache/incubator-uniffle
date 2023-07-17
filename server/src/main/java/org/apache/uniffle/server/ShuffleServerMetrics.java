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

package org.apache.uniffle.server;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.common.LocalStorage;

public class ShuffleServerMetrics {

  private static final String TOTAL_RECEIVED_DATA = "total_received_data";
  private static final String TOTAL_WRITE_DATA = "total_write_data";
  private static final String TOTAL_WRITE_BLOCK = "total_write_block";
  private static final String TOTAL_WRITE_TIME = "total_write_time";
  private static final String TOTAL_WRITE_HANDLER = "total_write_handler";
  private static final String TOTAL_WRITE_EXCEPTION = "total_write_exception";
  private static final String TOTAL_WRITE_SLOW = "total_write_slow";
  private static final String TOTAL_WRITE_NUM = "total_write_num";
  private static final String APP_NUM_WITH_NODE = "app_num_with_node";
  private static final String PARTITION_NUM_WITH_NODE = "partition_num_with_node";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL1 = "event_size_threshold_level1";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL2 = "event_size_threshold_level2";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL3 = "event_size_threshold_level3";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL4 = "event_size_threshold_level4";
  private static final String EVENT_QUEUE_SIZE = "event_queue_size";
  private static final String TOTAL_READ_DATA = "total_read_data";
  private static final String TOTAL_READ_LOCAL_DATA_FILE = "total_read_local_data_file";
  private static final String TOTAL_READ_LOCAL_INDEX_FILE = "total_read_local_index_file";
  private static final String TOTAL_READ_MEMORY_DATA = "total_read_memory_data";
  private static final String TOTAL_READ_TIME = "total_read_time";
  private static final String TOTAL_REQUIRE_READ_MEMORY = "total_require_read_memory_num";
  private static final String TOTAL_REQUIRE_READ_MEMORY_RETRY =
      "total_require_read_memory_retry_num";
  private static final String TOTAL_REQUIRE_READ_MEMORY_FAILED =
      "total_require_read_memory_failed_num";

  private static final String LOCAL_STORAGE_TOTAL_DIRS_NUM = "local_storage_total_dirs_num";
  private static final String LOCAL_STORAGE_CORRUPTED_DIRS_NUM = "local_storage_corrupted_dirs_num";
  private static final String LOCAL_STORAGE_TOTAL_SPACE = "local_storage_total_space";
  private static final String LOCAL_STORAGE_USED_SPACE = "local_storage_used_space";
  private static final String LOCAL_STORAGE_USED_SPACE_RATIO = "local_storage_used_space_ratio";

  private static final String IS_HEALTHY = "is_healthy";
  private static final String ALLOCATED_BUFFER_SIZE = "allocated_buffer_size";
  private static final String IN_FLUSH_BUFFER_SIZE = "in_flush_buffer_size";
  private static final String USED_BUFFER_SIZE = "used_buffer_size";
  private static final String READ_USED_BUFFER_SIZE = "read_used_buffer_size";
  private static final String TOTAL_FAILED_WRITTEN_EVENT_NUM = "total_failed_written_event_num";
  private static final String TOTAL_DROPPED_EVENT_NUM = "total_dropped_event_num";
  private static final String TOTAL_HADOOP_WRITE_DATA = "total_hadoop_write_data";
  private static final String TOTAL_LOCALFILE_WRITE_DATA = "total_localfile_write_data";
  private static final String TOTAL_REQUIRE_BUFFER_FAILED = "total_require_buffer_failed";
  private static final String TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION =
      "total_require_buffer_failed_for_huge_partition";
  private static final String TOTAL_REQUIRE_BUFFER_FAILED_FOR_REGULAR_PARTITION =
      "total_require_buffer_failed_for_regular_partition";
  private static final String STORAGE_TOTAL_WRITE_LOCAL = "storage_total_write_local";
  private static final String STORAGE_RETRY_WRITE_LOCAL = "storage_retry_write_local";
  private static final String STORAGE_FAILED_WRITE_LOCAL = "storage_failed_write_local";
  private static final String STORAGE_SUCCESS_WRITE_LOCAL = "storage_success_write_local";
  private static final String STORAGE_HOST_LABEL = "storage_host";
  public static final String STORAGE_TOTAL_WRITE_REMOTE = "storage_total_write_remote";
  public static final String STORAGE_RETRY_WRITE_REMOTE = "storage_retry_write_remote";
  public static final String STORAGE_FAILED_WRITE_REMOTE = "storage_failed_write_remote";
  public static final String STORAGE_SUCCESS_WRITE_REMOTE = "storage_success_write_remote";

  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String TOTAL_APP_WITH_HUGE_PARTITION_NUM =
      "total_app_with_huge_partition_num";
  private static final String TOTAL_PARTITION_NUM = "total_partition_num";
  private static final String TOTAL_HUGE_PARTITION_NUM = "total_huge_partition_num";

  private static final String HUGE_PARTITION_NUM = "huge_partition_num";
  private static final String APP_WITH_HUGE_PARTITION_NUM = "app_with_huge_partition_num";

  private static final String LOCAL_FILE_EVENT_FLUSH_NUM = "local_file_event_flush_num";
  private static final String HADOOP_EVENT_FLUSH_NUM = "hadoop_event_flush_num";

  public static Counter.Child counterTotalAppNum;
  public static Counter.Child counterTotalAppWithHugePartitionNum;
  public static Counter.Child counterTotalPartitionNum;
  public static Counter.Child counterTotalHugePartitionNum;

  public static Counter.Child counterTotalReceivedDataSize;
  public static Counter.Child counterTotalWriteDataSize;
  public static Counter.Child counterTotalWriteBlockSize;
  public static Counter.Child counterTotalWriteTime;
  public static Counter.Child counterWriteException;
  public static Counter.Child counterWriteSlow;
  public static Counter.Child counterWriteTotal;
  public static Counter.Child counterEventSizeThresholdLevel1;
  public static Counter.Child counterEventSizeThresholdLevel2;
  public static Counter.Child counterEventSizeThresholdLevel3;
  public static Counter.Child counterEventSizeThresholdLevel4;
  public static Counter.Child counterTotalReadDataSize;
  public static Counter.Child counterTotalReadLocalDataFileSize;
  public static Counter.Child counterTotalReadLocalIndexFileSize;
  public static Counter.Child counterTotalReadMemoryDataSize;
  public static Counter.Child counterTotalReadTime;
  public static Counter.Child counterTotalFailedWrittenEventNum;
  public static Counter.Child counterTotalDroppedEventNum;
  public static Counter.Child counterTotalHadoopWriteDataSize;
  public static Counter.Child counterTotalLocalFileWriteDataSize;
  public static Counter.Child counterTotalRequireBufferFailed;
  public static Counter.Child counterTotalRequireBufferFailedForHugePartition;
  public static Counter.Child counterTotalRequireBufferFailedForRegularPartition;

  public static Counter.Child counterLocalStorageTotalWrite;
  public static Counter.Child counterLocalStorageRetryWrite;
  public static Counter.Child counterLocalStorageFailedWrite;
  public static Counter.Child counterLocalStorageSuccessWrite;
  public static Counter.Child counterTotalRequireReadMemoryNum;
  public static Counter.Child counterTotalRequireReadMemoryRetryNum;
  public static Counter.Child counterTotalRequireReadMemoryFailedNum;

  public static Gauge.Child gaugeHugePartitionNum;
  public static Gauge.Child gaugeAppWithHugePartitionNum;

  public static Gauge.Child gaugeLocalStorageTotalDirsNum;
  public static Gauge.Child gaugeLocalStorageCorruptedDirsNum;
  public static Gauge.Child gaugeLocalStorageTotalSpace;
  public static Gauge.Child gaugeLocalStorageUsedSpace;
  public static Gauge.Child gaugeLocalStorageUsedSpaceRatio;

  public static Gauge.Child gaugeIsHealthy;
  public static Gauge.Child gaugeAllocatedBufferSize;
  public static Gauge.Child gaugeInFlushBufferSize;
  public static Gauge.Child gaugeUsedBufferSize;
  public static Gauge.Child gaugeReadBufferUsedSize;
  public static Gauge.Child gaugeWriteHandler;
  public static Gauge.Child gaugeEventQueueSize;
  public static Gauge.Child gaugeAppNum;
  public static Gauge.Child gaugeTotalPartitionNum;
  public static Counter counterRemoteStorageTotalWrite;
  public static Counter counterRemoteStorageRetryWrite;
  public static Counter counterRemoteStorageFailedWrite;
  public static Counter counterRemoteStorageSuccessWrite;
  private static String tags;
  public static Counter counterLocalFileEventFlush;
  public static Counter counterHadoopEventFlush;

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(CollectorRegistry collectorRegistry, String tags) {
    if (!isRegister) {
      ShuffleServerMetrics.tags = tags;
      Map<String, String> labels = Maps.newHashMap();
      labels.put(Constants.METRICS_TAG_LABEL_NAME, ShuffleServerMetrics.tags);
      metricsManager = new MetricsManager(collectorRegistry, labels);
      isRegister = true;
      setUpMetrics();
    }
  }

  @VisibleForTesting
  public static void register() {
    register(CollectorRegistry.defaultRegistry, Constants.SHUFFLE_SERVER_VERSION);
  }

  @VisibleForTesting
  public static void clear() {
    isRegister = false;
    CollectorRegistry.defaultRegistry.clear();
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public static void incStorageRetryCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageRetryWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageRetryWrite.labels(tags, storageHost).inc();
      }
    }
  }

  public static void incStorageSuccessCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageSuccessWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageSuccessWrite.labels(tags, storageHost).inc();
      }
    }
  }

  public static void incStorageFailedCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageFailedWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageFailedWrite.labels(tags, storageHost).inc();
      }
    }
  }

  private static void setUpMetrics() {
    counterTotalReceivedDataSize = metricsManager.addLabeledCounter(TOTAL_RECEIVED_DATA);
    counterTotalWriteDataSize = metricsManager.addLabeledCounter(TOTAL_WRITE_DATA);
    counterTotalWriteBlockSize = metricsManager.addLabeledCounter(TOTAL_WRITE_BLOCK);
    counterTotalWriteTime = metricsManager.addLabeledCounter(TOTAL_WRITE_TIME);
    counterWriteException = metricsManager.addLabeledCounter(TOTAL_WRITE_EXCEPTION);
    counterWriteSlow = metricsManager.addLabeledCounter(TOTAL_WRITE_SLOW);
    counterWriteTotal = metricsManager.addLabeledCounter(TOTAL_WRITE_NUM);
    counterEventSizeThresholdLevel1 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL1);
    counterEventSizeThresholdLevel2 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL2);
    counterEventSizeThresholdLevel3 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL3);
    counterEventSizeThresholdLevel4 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL4);
    counterTotalReadDataSize = metricsManager.addLabeledCounter(TOTAL_READ_DATA);
    counterTotalReadLocalDataFileSize =
        metricsManager.addLabeledCounter(TOTAL_READ_LOCAL_DATA_FILE);
    counterTotalReadLocalIndexFileSize =
        metricsManager.addLabeledCounter(TOTAL_READ_LOCAL_INDEX_FILE);
    counterTotalReadMemoryDataSize = metricsManager.addLabeledCounter(TOTAL_READ_MEMORY_DATA);
    counterTotalReadTime = metricsManager.addLabeledCounter(TOTAL_READ_TIME);
    counterTotalDroppedEventNum = metricsManager.addLabeledCounter(TOTAL_DROPPED_EVENT_NUM);
    counterTotalFailedWrittenEventNum =
        metricsManager.addLabeledCounter(TOTAL_FAILED_WRITTEN_EVENT_NUM);
    counterTotalHadoopWriteDataSize = metricsManager.addLabeledCounter(TOTAL_HADOOP_WRITE_DATA);
    counterTotalLocalFileWriteDataSize =
        metricsManager.addLabeledCounter(TOTAL_LOCALFILE_WRITE_DATA);
    counterTotalRequireBufferFailed = metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED);
    counterTotalRequireBufferFailedForRegularPartition =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_REGULAR_PARTITION);
    counterTotalRequireBufferFailedForHugePartition =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION);
    counterLocalStorageTotalWrite = metricsManager.addLabeledCounter(STORAGE_TOTAL_WRITE_LOCAL);
    counterLocalStorageRetryWrite = metricsManager.addLabeledCounter(STORAGE_RETRY_WRITE_LOCAL);
    counterLocalStorageFailedWrite = metricsManager.addLabeledCounter(STORAGE_FAILED_WRITE_LOCAL);
    counterLocalStorageSuccessWrite = metricsManager.addLabeledCounter(STORAGE_SUCCESS_WRITE_LOCAL);
    counterRemoteStorageTotalWrite =
        metricsManager.addCounter(
            STORAGE_TOTAL_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageRetryWrite =
        metricsManager.addCounter(
            STORAGE_RETRY_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageFailedWrite =
        metricsManager.addCounter(
            STORAGE_FAILED_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageSuccessWrite =
        metricsManager.addCounter(
            STORAGE_SUCCESS_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterTotalRequireReadMemoryNum = metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY);
    counterTotalRequireReadMemoryRetryNum =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY_RETRY);
    counterTotalRequireReadMemoryFailedNum =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY_FAILED);

    counterTotalAppNum = metricsManager.addLabeledCounter(TOTAL_APP_NUM);
    counterTotalAppWithHugePartitionNum =
        metricsManager.addLabeledCounter(TOTAL_APP_WITH_HUGE_PARTITION_NUM);
    counterTotalPartitionNum = metricsManager.addLabeledCounter(TOTAL_PARTITION_NUM);
    counterTotalHugePartitionNum = metricsManager.addLabeledCounter(TOTAL_HUGE_PARTITION_NUM);

    gaugeLocalStorageTotalDirsNum = metricsManager.addLabeledGauge(LOCAL_STORAGE_TOTAL_DIRS_NUM);
    gaugeLocalStorageCorruptedDirsNum =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_CORRUPTED_DIRS_NUM);
    gaugeLocalStorageTotalSpace = metricsManager.addLabeledGauge(LOCAL_STORAGE_TOTAL_SPACE);
    gaugeLocalStorageUsedSpace = metricsManager.addLabeledGauge(LOCAL_STORAGE_USED_SPACE);
    gaugeLocalStorageUsedSpaceRatio =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_USED_SPACE_RATIO);

    gaugeIsHealthy = metricsManager.addLabeledGauge(IS_HEALTHY);
    gaugeAllocatedBufferSize = metricsManager.addLabeledGauge(ALLOCATED_BUFFER_SIZE);
    gaugeInFlushBufferSize = metricsManager.addLabeledGauge(IN_FLUSH_BUFFER_SIZE);
    gaugeUsedBufferSize = metricsManager.addLabeledGauge(USED_BUFFER_SIZE);
    gaugeReadBufferUsedSize = metricsManager.addLabeledGauge(READ_USED_BUFFER_SIZE);
    gaugeWriteHandler = metricsManager.addLabeledGauge(TOTAL_WRITE_HANDLER);
    gaugeEventQueueSize = metricsManager.addLabeledGauge(EVENT_QUEUE_SIZE);
    gaugeAppNum = metricsManager.addLabeledGauge(APP_NUM_WITH_NODE);
    gaugeTotalPartitionNum = metricsManager.addLabeledGauge(PARTITION_NUM_WITH_NODE);

    gaugeHugePartitionNum = metricsManager.addLabeledGauge(HUGE_PARTITION_NUM);
    gaugeAppWithHugePartitionNum = metricsManager.addLabeledGauge(APP_WITH_HUGE_PARTITION_NUM);

    counterLocalFileEventFlush = metricsManager.addCounter(LOCAL_FILE_EVENT_FLUSH_NUM);
    counterHadoopEventFlush = metricsManager.addCounter(HADOOP_EVENT_FLUSH_NUM);
  }
}
