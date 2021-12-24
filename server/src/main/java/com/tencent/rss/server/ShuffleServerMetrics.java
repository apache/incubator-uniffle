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

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

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

  private static final String REGISTERED_SHUFFLE = "registered_shuffle";
  private static final String REGISTERED_SHUFFLE_ENGINE = "registered_shuffle_engine";
  private static final String BUFFERED_DATA_SIZE = "buffered_data_size";
  private static final String ALLOCATED_BUFFER_SIZE = "allocated_buffer_size";
  private static final String IN_FLUSH_BUFFER_SIZE = "in_flush_buffer_size";
  private static final String USED_BUFFER_SIZE = "used_buffer_size";
  private static final String TOTAL_UPLOAD_SIZE = "total_upload_size";
  private static final String TOTAL_UPLOAD_TIME_S = "total_upload_time_s";
  private static final String TOTAL_DROPPED_EVENT_NUM = "total_dropped_event_num";

  public static Counter counterTotalReceivedDataSize;
  public static Counter counterTotalWriteDataSize;
  public static Counter counterTotalWriteBlockSize;
  public static Counter counterTotalWriteTime;
  public static Counter counterWriteException;
  public static Counter counterWriteSlow;
  public static Counter counterWriteTotal;
  public static Counter counterEventSizeThresholdLevel1;
  public static Counter counterEventSizeThresholdLevel2;
  public static Counter counterEventSizeThresholdLevel3;
  public static Counter counterEventSizeThresholdLevel4;
  public static Counter counterTotalReadDataSize;
  public static Counter counterTotalReadLocalDataFileSize;
  public static Counter counterTotalReadLocalIndexFileSize;
  public static Counter counterTotalReadMemoryDataSize;
  public static Counter counterTotalReadTime;
  public static Counter counterTotalUploadSize;
  public static Counter counterTotalUploadTimeS;
  public static Counter counterTotalDroppedEventNum;

  public static Gauge gaugeRegisteredShuffle;
  public static Gauge gaugeRegisteredShuffleEngine;
  public static Gauge gaugeBufferDataSize;
  public static Gauge gaugeAllocatedBufferSize;
  public static Gauge gaugeInFlushBufferSize;
  public static Gauge gaugeUsedBufferSize;
  public static Gauge gaugeWriteHandler;
  public static Gauge gaugeEventQueueSize;
  public static Gauge gaugeAppNum;
  public static Gauge gaugeTotalPartitionNum;

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      isRegister = true;
      setUpMetrics();
    }
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  private static void setUpMetrics() {
    counterTotalReceivedDataSize = metricsManager.addCounter(TOTAL_RECEIVED_DATA);
    counterTotalWriteDataSize = metricsManager.addCounter(TOTAL_WRITE_DATA);
    counterTotalWriteBlockSize = metricsManager.addCounter(TOTAL_WRITE_BLOCK);
    counterTotalWriteTime = metricsManager.addCounter(TOTAL_WRITE_TIME);
    counterWriteException = metricsManager.addCounter(TOTAL_WRITE_EXCEPTION);
    counterWriteSlow = metricsManager.addCounter(TOTAL_WRITE_SLOW);
    counterWriteTotal = metricsManager.addCounter(TOTAL_WRITE_NUM);
    counterEventSizeThresholdLevel1 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL1);
    counterEventSizeThresholdLevel2 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL2);
    counterEventSizeThresholdLevel3 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL3);
    counterEventSizeThresholdLevel4 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL4);
    counterTotalReadDataSize = metricsManager.addCounter(TOTAL_READ_DATA);
    counterTotalReadLocalDataFileSize = metricsManager.addCounter(TOTAL_READ_LOCAL_DATA_FILE);
    counterTotalReadLocalIndexFileSize = metricsManager.addCounter(TOTAL_READ_LOCAL_INDEX_FILE);
    counterTotalReadMemoryDataSize = metricsManager.addCounter(TOTAL_READ_MEMORY_DATA);
    counterTotalReadTime = metricsManager.addCounter(TOTAL_READ_TIME);
    counterTotalUploadSize = metricsManager.addCounter(TOTAL_UPLOAD_SIZE);
    counterTotalUploadTimeS = metricsManager.addCounter(TOTAL_UPLOAD_TIME_S);
    counterTotalDroppedEventNum = metricsManager.addCounter(TOTAL_DROPPED_EVENT_NUM);

    gaugeRegisteredShuffle = metricsManager.addGauge(REGISTERED_SHUFFLE);
    gaugeRegisteredShuffleEngine = metricsManager.addGauge(REGISTERED_SHUFFLE_ENGINE);
    gaugeBufferDataSize = metricsManager.addGauge(BUFFERED_DATA_SIZE);
    gaugeAllocatedBufferSize = metricsManager.addGauge(ALLOCATED_BUFFER_SIZE);
    gaugeInFlushBufferSize = metricsManager.addGauge(IN_FLUSH_BUFFER_SIZE);
    gaugeUsedBufferSize = metricsManager.addGauge(USED_BUFFER_SIZE);
    gaugeWriteHandler = metricsManager.addGauge(TOTAL_WRITE_HANDLER);
    gaugeEventQueueSize = metricsManager.addGauge(EVENT_QUEUE_SIZE);
    gaugeAppNum = metricsManager.addGauge(APP_NUM_WITH_NODE);
    gaugeTotalPartitionNum = metricsManager.addGauge(PARTITION_NUM_WITH_NODE);
  }

}
