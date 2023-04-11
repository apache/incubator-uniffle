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

import com.google.common.annotations.VisibleForTesting;
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
  private static final String TOTAL_REQUIRE_READ_MEMORY_RETRY = "total_require_read_memory_retry_num";
  private static final String TOTAL_REQUIRE_READ_MEMORY_FAILED = "total_require_read_memory_failed_num";

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
  private static final String TOTAL_HDFS_WRITE_DATA = "total_hdfs_write_data";
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
  private static final String TOTAL_APP_WITH_HUGE_PARTITION_NUM = "total_app_with_huge_partition_num";
  private static final String TOTAL_PARTITION_NUM = "total_partition_num";
  private static final String TOTAL_HUGE_PARTITION_NUM = "total_huge_partition_num";

  private static final String HUGE_PARTITION_NUM = "huge_partition_num";
  private static final String APP_WITH_HUGE_PARTITION_NUM = "app_with_huge_partition_num";

  public static Counter.Child counterTotalAppNum;
  public static Counter.Child counterTotalAppWithHugePartitionNum;
  public static Counter.Child counterTotalPartitionNum;
  public static Counter.Child counterTotalHugePartitionNum;

  public static Counter.Child  counterTotalReceivedDataSize;
  public static Counter.Child  counterTotalWriteDataSize;
  public static Counter.Child  counterTotalWriteBlockSize;
  public static Counter.Child  counterTotalWriteTime;
  public static Counter.Child  counterWriteException;
  public static Counter.Child  counterWriteSlow;
  public static Counter.Child  counterWriteTotal;
  public static Counter.Child  counterEventSizeThresholdLevel1;
  public static Counter.Child  counterEventSizeThresholdLevel2;
  public static Counter.Child  counterEventSizeThresholdLevel3;
  public static Counter.Child  counterEventSizeThresholdLevel4;
  public static Counter.Child  counterTotalReadDataSize;
  public static Counter.Child  counterTotalReadLocalDataFileSize;
  public static Counter.Child  counterTotalReadLocalIndexFileSize;
  public static Counter.Child  counterTotalReadMemoryDataSize;
  public static Counter.Child  counterTotalReadTime;
  public static Counter.Child  counterTotalFailedWrittenEventNum;
  public static Counter.Child  counterTotalDroppedEventNum;
  public static Counter.Child  counterTotalHdfsWriteDataSize;
  public static Counter.Child  counterTotalLocalFileWriteDataSize;
  public static Counter.Child  counterTotalRequireBufferFailed;
  public static Counter.Child  counterTotalRequireBufferFailedForHugePartition;
  public static Counter.Child  counterTotalRequireBufferFailedForRegularPartition;

  public static Counter.Child  counterLocalStorageTotalWrite;
  public static Counter.Child  counterLocalStorageRetryWrite;
  public static Counter.Child  counterLocalStorageFailedWrite;
  public static Counter.Child  counterLocalStorageSuccessWrite;
  public static Counter.Child  counterTotalRequireReadMemoryNum;
  public static Counter.Child  counterTotalRequireReadMemoryRetryNum;
  public static Counter.Child  counterTotalRequireReadMemoryFailedNum;

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

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(CollectorRegistry collectorRegistry, String tags) {
    if (!isRegister) {
      ShuffleServerMetrics.tags = tags;
      metricsManager = new MetricsManager(collectorRegistry);
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
    counterTotalReceivedDataSize = addCounter(TOTAL_RECEIVED_DATA).labels(tags);
    counterTotalWriteDataSize = addCounter(TOTAL_WRITE_DATA).labels(tags);
    counterTotalWriteBlockSize = addCounter(TOTAL_WRITE_BLOCK).labels(tags);
    counterTotalWriteTime = addCounter(TOTAL_WRITE_TIME).labels(tags);
    counterWriteException = addCounter(TOTAL_WRITE_EXCEPTION).labels(tags);
    counterWriteSlow = addCounter(TOTAL_WRITE_SLOW).labels(tags);
    counterWriteTotal = addCounter(TOTAL_WRITE_NUM).labels(tags);
    counterEventSizeThresholdLevel1 = addCounter(EVENT_SIZE_THRESHOLD_LEVEL1).labels(tags);
    counterEventSizeThresholdLevel2 = addCounter(EVENT_SIZE_THRESHOLD_LEVEL2).labels(tags);
    counterEventSizeThresholdLevel3 = addCounter(EVENT_SIZE_THRESHOLD_LEVEL3).labels(tags);
    counterEventSizeThresholdLevel4 = addCounter(EVENT_SIZE_THRESHOLD_LEVEL4).labels(tags);
    counterTotalReadDataSize = addCounter(TOTAL_READ_DATA).labels(tags);
    counterTotalReadLocalDataFileSize = addCounter(TOTAL_READ_LOCAL_DATA_FILE).labels(tags);
    counterTotalReadLocalIndexFileSize = addCounter(TOTAL_READ_LOCAL_INDEX_FILE).labels(tags);
    counterTotalReadMemoryDataSize = addCounter(TOTAL_READ_MEMORY_DATA).labels(tags);
    counterTotalReadTime = addCounter(TOTAL_READ_TIME).labels(tags);
    counterTotalDroppedEventNum = addCounter(TOTAL_DROPPED_EVENT_NUM).labels(tags);
    counterTotalFailedWrittenEventNum = addCounter(TOTAL_FAILED_WRITTEN_EVENT_NUM).labels(tags);
    counterTotalHdfsWriteDataSize = addCounter(TOTAL_HDFS_WRITE_DATA).labels(tags);
    counterTotalLocalFileWriteDataSize = addCounter(TOTAL_LOCALFILE_WRITE_DATA).labels(tags);
    counterTotalRequireBufferFailed = addCounter(TOTAL_REQUIRE_BUFFER_FAILED).labels(tags);
    counterTotalRequireBufferFailedForRegularPartition =
        addCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_REGULAR_PARTITION).labels(tags);
    counterTotalRequireBufferFailedForHugePartition =
        addCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION).labels(tags);
    counterLocalStorageTotalWrite = addCounter(STORAGE_TOTAL_WRITE_LOCAL).labels(tags);
    counterLocalStorageRetryWrite = addCounter(STORAGE_RETRY_WRITE_LOCAL).labels(tags);
    counterLocalStorageFailedWrite = addCounter(STORAGE_FAILED_WRITE_LOCAL).labels(tags);
    counterLocalStorageSuccessWrite = addCounter(STORAGE_SUCCESS_WRITE_LOCAL).labels(tags);
    counterRemoteStorageTotalWrite = metricsManager.addCounter(
        STORAGE_TOTAL_WRITE_REMOTE, Constants.SHUFFLE_SERVER_TAGS, STORAGE_HOST_LABEL);
    counterRemoteStorageRetryWrite = metricsManager.addCounter(
        STORAGE_RETRY_WRITE_REMOTE, Constants.SHUFFLE_SERVER_TAGS, STORAGE_HOST_LABEL);
    counterRemoteStorageFailedWrite = metricsManager.addCounter(
        STORAGE_FAILED_WRITE_REMOTE, Constants.SHUFFLE_SERVER_TAGS, STORAGE_HOST_LABEL);
    counterRemoteStorageSuccessWrite = metricsManager.addCounter(
        STORAGE_SUCCESS_WRITE_REMOTE, Constants.SHUFFLE_SERVER_TAGS, STORAGE_HOST_LABEL);
    counterTotalRequireReadMemoryNum = addCounter(TOTAL_REQUIRE_READ_MEMORY).labels(tags);
    counterTotalRequireReadMemoryRetryNum = addCounter(TOTAL_REQUIRE_READ_MEMORY_RETRY).labels(tags);
    counterTotalRequireReadMemoryFailedNum = addCounter(TOTAL_REQUIRE_READ_MEMORY_FAILED).labels(tags);

    counterTotalAppNum = addCounter(TOTAL_APP_NUM).labels(tags);
    counterTotalAppWithHugePartitionNum = addCounter(TOTAL_APP_WITH_HUGE_PARTITION_NUM).labels(tags);
    counterTotalPartitionNum = addCounter(TOTAL_PARTITION_NUM).labels(tags);
    counterTotalHugePartitionNum = addCounter(TOTAL_HUGE_PARTITION_NUM).labels(tags);

    gaugeLocalStorageTotalDirsNum = addGauge(LOCAL_STORAGE_TOTAL_DIRS_NUM).labels(tags);
    gaugeLocalStorageCorruptedDirsNum = addGauge(LOCAL_STORAGE_CORRUPTED_DIRS_NUM).labels(tags);
    gaugeLocalStorageTotalSpace = addGauge(LOCAL_STORAGE_TOTAL_SPACE).labels(tags);
    gaugeLocalStorageUsedSpace = addGauge(LOCAL_STORAGE_USED_SPACE).labels(tags);
    gaugeLocalStorageUsedSpaceRatio = addGauge(LOCAL_STORAGE_USED_SPACE_RATIO).labels(tags);

    gaugeIsHealthy = addGauge(IS_HEALTHY).labels(tags);
    gaugeAllocatedBufferSize = addGauge(ALLOCATED_BUFFER_SIZE).labels(tags);
    gaugeInFlushBufferSize = addGauge(IN_FLUSH_BUFFER_SIZE).labels(tags);
    gaugeUsedBufferSize = addGauge(USED_BUFFER_SIZE).labels(tags);
    gaugeReadBufferUsedSize = addGauge(READ_USED_BUFFER_SIZE).labels(tags);
    gaugeWriteHandler = addGauge(TOTAL_WRITE_HANDLER).labels(tags);
    gaugeEventQueueSize = addGauge(EVENT_QUEUE_SIZE).labels(tags);
    gaugeAppNum = addGauge(APP_NUM_WITH_NODE).labels(tags);
    gaugeTotalPartitionNum = addGauge(PARTITION_NUM_WITH_NODE).labels(tags);

    gaugeHugePartitionNum = addGauge(HUGE_PARTITION_NUM).labels(tags);
    gaugeAppWithHugePartitionNum = addGauge(APP_WITH_HUGE_PARTITION_NUM).labels(tags);
  }

  private static Counter addCounter(String grpcTotal) {
    return metricsManager.addCounter(grpcTotal, Constants.SHUFFLE_SERVER_TAGS);
  }

  private static Gauge addGauge(String name) {
    return metricsManager.addGauge(name, Constants.SHUFFLE_SERVER_TAGS);
  }
}
