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

import org.apache.uniffle.common.metrics.GRPCMetrics;

public class ShuffleServerGrpcMetrics extends GRPCMetrics {

  public static final String REGISTER_SHUFFLE_METHOD = "registerShuffle";
  public static final String SEND_SHUFFLE_DATA_METHOD = "sendShuffleData";
  public static final String COMMIT_SHUFFLE_TASK_METHOD = "commitShuffleTask";
  public static final String FINISH_SHUFFLE_METHOD = "finishShuffle";
  public static final String REQUIRE_BUFFER_METHOD = "requireBuffer";
  public static final String APP_HEARTBEAT_METHOD = "appHeartbeat";
  public static final String REPORT_SHUFFLE_RESULT_METHOD = "reportShuffleResult";
  public static final String GET_SHUFFLE_RESULT_METHOD = "getShuffleResult";
  public static final String GET_SHUFFLE_DATA_METHOD = "getLocalShuffleData";
  public static final String GET_MEMORY_SHUFFLE_DATA_METHOD = "getMemoryShuffleData";
  public static final String GET_SHUFFLE_INDEX_METHOD = "getLocalShuffleIndex";

  private static final String GRPC_REGISTERED_SHUFFLE = "grpc_registered_shuffle";
  private static final String GRPC_SEND_SHUFFLE_DATA = "grpc_send_shuffle_data";
  private static final String GRPC_COMMIT_SHUFFLE_TASK = "grpc_commit_shuffle_task";
  private static final String GRPC_FINISH_SHUFFLE = "grpc_finish_shuffle";
  private static final String GRPC_REQUIRE_BUFFER = "grpc_require_buffer";
  private static final String GRPC_APP_HEARTBEAT = "grpc_app_heartbeat";
  private static final String GRPC_REPORT_SHUFFLE_RESULT = "grpc_report_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_RESULT = "grpc_get_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_DATA = "grpc_get_local_shuffle_data";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA = "grpc_get_memory_shuffle_data";
  private static final String GRPC_GET_SHUFFLE_INDEX = "grpc_get_local_shuffle_index";

  private static final String GRPC_REGISTERED_SHUFFLE_TOTAL = "grpc_registered_shuffle_total";
  private static final String GRPC_SEND_SHUFFLE_DATA_TOTAL = "grpc_send_shuffle_data_total";
  private static final String GRPC_COMMIT_SHUFFLE_TASK_TOTAL = "grpc_commit_shuffle_task_total";
  private static final String GRPC_FINISH_SHUFFLE_TOTAL = "grpc_finish_shuffle_total";
  private static final String GRPC_REQUIRE_BUFFER_TOTAL = "grpc_require_buffer_total";
  private static final String GRPC_APP_HEARTBEAT_TOTAL = "grpc_app_heartbeat_total";
  private static final String GRPC_REPORT_SHUFFLE_RESULT_TOTAL = "grpc_report_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_RESULT_TOTAL = "grpc_get_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_DATA_TOTAL = "grpc_get_local_shuffle_data_total";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_TOTAL =
      "grpc_get_memory_shuffle_data_total";
  private static final String GRPC_GET_SHUFFLE_INDEX_TOTAL = "grpc_get_local_shuffle_index_total";

  private static final String GRPC_SEND_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_send_shuffle_data_transport_latency";
  private static final String GRPC_GET_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_get_local_shuffle_data_transport_latency";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_get_memory_shuffle_data_transport_latency";

  private static final String GRPC_SEND_SHUFFLE_DATA_PROCESS_LATENCY = "grpc_send_shuffle_data_process_latency";
  private static final String GRPC_GET_SHUFFLE_DATA_PROCESS_LATENCY = "grpc_get_local_shuffle_data_process_latency";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_PROCESS_LATENCY =
      "grpc_get_memory_shuffle_data_process_latency";

  public ShuffleServerGrpcMetrics(String tags) {
    super(tags);
  }

  @Override
  public void registerMetrics() {
    gaugeMap.putIfAbsent(REGISTER_SHUFFLE_METHOD,
        addGauge(GRPC_REGISTERED_SHUFFLE).labels(tags));
    gaugeMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        addGauge(GRPC_SEND_SHUFFLE_DATA).labels(tags));
    gaugeMap.putIfAbsent(COMMIT_SHUFFLE_TASK_METHOD,
        addGauge(GRPC_COMMIT_SHUFFLE_TASK).labels(tags));
    gaugeMap.putIfAbsent(FINISH_SHUFFLE_METHOD,
        addGauge(GRPC_FINISH_SHUFFLE).labels(tags));
    gaugeMap.putIfAbsent(REQUIRE_BUFFER_METHOD,
        addGauge(GRPC_REQUIRE_BUFFER).labels(tags));
    gaugeMap.putIfAbsent(APP_HEARTBEAT_METHOD,
        addGauge(GRPC_APP_HEARTBEAT).labels(tags));
    gaugeMap.putIfAbsent(REPORT_SHUFFLE_RESULT_METHOD,
        addGauge(GRPC_REPORT_SHUFFLE_RESULT).labels(tags));
    gaugeMap.putIfAbsent(GET_SHUFFLE_RESULT_METHOD,
        addGauge(GRPC_GET_SHUFFLE_RESULT).labels(tags));
    gaugeMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        addGauge(GRPC_GET_SHUFFLE_DATA).labels(tags));
    gaugeMap.putIfAbsent(GET_MEMORY_SHUFFLE_DATA_METHOD,
        addGauge(GRPC_GET_MEMORY_SHUFFLE_DATA).labels(tags));
    gaugeMap.putIfAbsent(GET_SHUFFLE_INDEX_METHOD,
        addGauge(GRPC_GET_SHUFFLE_INDEX).labels(tags));

    counterMap.putIfAbsent(REGISTER_SHUFFLE_METHOD,
        addCounter(GRPC_REGISTERED_SHUFFLE_TOTAL).labels(tags));
    counterMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        addCounter(GRPC_SEND_SHUFFLE_DATA_TOTAL).labels(tags));
    counterMap.putIfAbsent(COMMIT_SHUFFLE_TASK_METHOD,
        addCounter(GRPC_COMMIT_SHUFFLE_TASK_TOTAL).labels(tags));
    counterMap.putIfAbsent(FINISH_SHUFFLE_METHOD,
        addCounter(GRPC_FINISH_SHUFFLE_TOTAL).labels(tags));
    counterMap.putIfAbsent(REQUIRE_BUFFER_METHOD,
        addCounter(GRPC_REQUIRE_BUFFER_TOTAL).labels(tags));
    counterMap.putIfAbsent(APP_HEARTBEAT_METHOD,
        addCounter(GRPC_APP_HEARTBEAT_TOTAL).labels(tags));
    counterMap.putIfAbsent(REPORT_SHUFFLE_RESULT_METHOD,
        addCounter(GRPC_REPORT_SHUFFLE_RESULT_TOTAL).labels(tags));
    counterMap.putIfAbsent(GET_SHUFFLE_RESULT_METHOD,
        addCounter(GRPC_GET_SHUFFLE_RESULT_TOTAL).labels(tags));
    counterMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        addCounter(GRPC_GET_SHUFFLE_DATA_TOTAL).labels(tags));
    counterMap.putIfAbsent(GET_MEMORY_SHUFFLE_DATA_METHOD,
        addCounter(GRPC_GET_MEMORY_SHUFFLE_DATA_TOTAL).labels(tags));
    counterMap.putIfAbsent(GET_SHUFFLE_INDEX_METHOD,
        addCounter(GRPC_GET_SHUFFLE_INDEX_TOTAL).labels(tags));

    transportTimeSummaryMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_SEND_SHUFFLE_DATA_TRANSPORT_LATENCY).labels(tags));
    transportTimeSummaryMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_GET_SHUFFLE_DATA_TRANSPORT_LATENCY).labels(tags));
    transportTimeSummaryMap.putIfAbsent(GET_MEMORY_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_GET_MEMORY_SHUFFLE_DATA_TRANSPORT_LATENCY).labels(tags));

    processTimeSummaryMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_SEND_SHUFFLE_DATA_PROCESS_LATENCY).labels(tags));
    processTimeSummaryMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_GET_SHUFFLE_DATA_PROCESS_LATENCY).labels(tags));
    processTimeSummaryMap.putIfAbsent(GET_MEMORY_SHUFFLE_DATA_METHOD,
        addSummary(GRPC_GET_MEMORY_SHUFFLE_DATA_PROCESS_LATENCY).labels(tags));
  }

}
