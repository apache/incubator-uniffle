// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::config::MetricsConfig;
use crate::runtime::manager::RuntimeManager;
use log::{error, info};
use once_cell::sync::Lazy;
use prometheus::{
    histogram_opts, labels, register_histogram_vec_with_registry, register_int_gauge_vec,
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntGauge, IntGaugeVec, Registry,
};
use std::time::Duration;

const DEFAULT_BUCKETS: &[f64; 16] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0,
];

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static TOTAL_RECEIVED_DATA: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_received_data", "Incoming Requests").expect("metric should be created")
});

pub static TOTAL_READ_DATA: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_read_data", "Reading Data").expect("metric should be created")
});

pub static TOTAL_READ_DATA_FROM_MEMORY: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_read_data_from_memory", "Reading Data from memory")
        .expect("metric should be created")
});

pub static TOTAL_READ_DATA_FROM_LOCALFILE: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_read_data_from_localfile",
        "Reading Data from localfile",
    )
    .expect("metric should be created")
});

pub static GRPC_GET_MEMORY_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_memory_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_MEMORY_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_memory_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));
    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_localfile_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_GET_LOCALFILE_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_get_localfile_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_SEND_DATA_TRANSPORT_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_send_data_transport_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_SEND_DATA_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_send_data_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_BUFFER_REQUIRE_PROCESS_TIME: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("grpc_buffer_require_process_time", "none")
        .buckets(Vec::from(DEFAULT_BUCKETS as &'static [f64]));

    let histogram = Histogram::with_opts(opts).unwrap();
    histogram
});

pub static GRPC_LATENCY_TIME_SEC: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = histogram_opts!(
        "grpc_duration_seconds",
        "gRPC latency",
        Vec::from(DEFAULT_BUCKETS as &'static [f64])
    );
    let grpc_latency = register_histogram_vec_with_registry!(opts, &["path"], REGISTRY).unwrap();
    grpc_latency
});

pub static TOTAL_MEMORY_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_used", "Total memory used").expect("metric should be created")
});

pub static TOTAL_LOCALFILE_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_localfile_used", "Total localfile used")
        .expect("metric should be created")
});

pub static TOTAL_HDFS_USED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_hdfs_used", "Total hdfs used").expect("metric should be created")
});
pub static GAUGE_MEMORY_USED: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("memory_used", "memory used").expect("metric should be created"));
pub static GAUGE_MEMORY_ALLOCATED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_allocated", "memory allocated").expect("metric should be created")
});
pub static GAUGE_MEMORY_CAPACITY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_capacity", "memory capacity").expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_OPERATION: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill", "memory capacity").expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_OPERATION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill_failed", "memory capacity")
        .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_LOCALFILE: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_memory_spill_to_localfile",
        "memory spill to localfile",
    )
    .expect("metric should be created")
});
pub static TOTAL_MEMORY_SPILL_TO_HDFS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_memory_spill_to_hdfs", "memory spill to hdfs")
        .expect("metric should be created")
});
pub static GAUGE_MEMORY_SPILL_OPERATION: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("memory_spill", "memory spill").expect("metric should be created"));
pub static GAUGE_MEMORY_SPILL_TO_LOCALFILE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_spill_to_localfile", "memory spill to localfile")
        .expect("metric should be created")
});
pub static GAUGE_MEMORY_SPILL_TO_HDFS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("memory_spill_to_hdfs", "memory spill to hdfs").expect("metric should be created")
});
pub static TOTAL_APP_NUMBER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_app_number", "total_app_number").expect("metrics should be created")
});
pub static TOTAL_PARTITION_NUMBER: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_partition_number", "total_partition_number")
        .expect("metrics should be created")
});
pub static GAUGE_APP_NUMBER: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("app_number", "app_number").expect("metrics should be created"));
pub static GAUGE_PARTITION_NUMBER: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("partition_number", "partition_number").expect("metrics should be created")
});
pub static TOTAL_REQUIRE_BUFFER_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("total_require_buffer_failed", "total_require_buffer_failed")
        .expect("metrics should be created")
});
pub static TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_huge_partition_require_buffer_failed",
        "total_huge_partition_require_buffer_failed",
    )
    .expect("metrics should be created")
});

pub static GAUGE_LOCAL_DISK_CAPACITY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_capacity",
        "local disk capacity for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_USED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_used",
        "local disk used for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_LOCAL_DISK_IS_HEALTHY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "local_disk_is_healthy",
        "local disk is_healthy for root path",
        &["root"]
    )
    .unwrap()
});

pub static GAUGE_RUNTIME_ALIVE_THREAD_NUM: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "runtime_thread_alive_gauge",
        "alive thread number for runtime",
        &["name"]
    )
    .unwrap()
});

pub static GAUGE_RUNTIME_IDLE_THREAD_NUM: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "runtime_thread_idle_gauge",
        "idle thread number for runtime",
        &["name"]
    )
    .unwrap()
});

pub static GAUGE_TOPN_APP_RESIDENT_DATA_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "topN_app_resident_data_size",
        "topN app resident data size",
        &["app_id"]
    )
    .unwrap()
});

pub static GAUGE_IN_SPILL_DATA_SIZE: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("in_spill_data_size", "total data size in spill").unwrap());

pub static GAUGE_GRPC_REQUEST_QUEUE_SIZE: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("grpc_request_queue_size", "grpc request queue size").unwrap());

pub static TOTAL_SPILL_EVENTS_DROPPED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new(
        "total_spill_events_dropped",
        "total spill events dropped number",
    )
    .expect("")
});

fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(TOTAL_SPILL_EVENTS_DROPPED.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_TOPN_APP_RESIDENT_DATA_SIZE.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_READ_DATA_FROM_LOCALFILE.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(TOTAL_READ_DATA_FROM_MEMORY.clone()))
        .expect("total_read_data must be registered");

    REGISTRY
        .register(Box::new(GAUGE_IN_SPILL_DATA_SIZE.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_CAPACITY.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_USED.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_LOCAL_DISK_IS_HEALTHY.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_RUNTIME_ALIVE_THREAD_NUM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(GAUGE_RUNTIME_IDLE_THREAD_NUM.clone()))
        .expect("");

    REGISTRY
        .register(Box::new(TOTAL_RECEIVED_DATA.clone()))
        .expect("total_received_data must be registered");
    REGISTRY
        .register(Box::new(TOTAL_READ_DATA.clone()))
        .expect("total_read_data must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_USED.clone()))
        .expect("total_memory_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_LOCALFILE_USED.clone()))
        .expect("total_localfile_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HDFS_USED.clone()))
        .expect("total_hdfs_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION.clone()))
        .expect("total_memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION_FAILED.clone()))
        .expect("total_memory_spill_operation_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_APP_NUMBER.clone()))
        .expect("total_app_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_PARTITION_NUMBER.clone()))
        .expect("total_partition_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_huge_partition_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("total_memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("total_memory_spill_to_hdfs must be registered");

    REGISTRY
        .register(Box::new(GAUGE_MEMORY_USED.clone()))
        .expect("memory_used must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_ALLOCATED.clone()))
        .expect("memory_allocated must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_CAPACITY.clone()))
        .expect("memory_capacity must be registered");
    REGISTRY
        .register(Box::new(GAUGE_APP_NUMBER.clone()))
        .expect("app_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_PARTITION_NUMBER.clone()))
        .expect("partition_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_OPERATION.clone()))
        .expect("memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("memory_spill_to_hdfs must be registered");
    REGISTRY
        .register(Box::new(GRPC_BUFFER_REQUIRE_PROCESS_TIME.clone()))
        .expect("grpc_buffer_require_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_SEND_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_send_data_transport_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_SEND_DATA_PROCESS_TIME.clone()))
        .expect("grpc_send_data_process_time must be registered");

    REGISTRY
        .register(Box::new(GRPC_GET_MEMORY_DATA_PROCESS_TIME.clone()))
        .expect("grpc_get_memory_data_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_LOCALFILE_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_get_localfile_data_transport_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_LOCALFILE_DATA_PROCESS_TIME.clone()))
        .expect("grpc_get_localfile_data_process_time must be registered");
    REGISTRY
        .register(Box::new(GRPC_GET_MEMORY_DATA_TRANSPORT_TIME.clone()))
        .expect("grpc_get_memory_data_transport_time must be registered");
}

pub fn init_metric_service(
    runtime_manager: RuntimeManager,
    metric_config: &Option<MetricsConfig>,
    worker_uid: String,
) -> bool {
    if metric_config.is_none() {
        return false;
    }

    register_custom_metrics();

    let job_name = "uniffle-worker";

    let cfg = metric_config.clone().unwrap();

    let push_gateway_endpoint = cfg.push_gateway_endpoint;

    if let Some(ref _endpoint) = push_gateway_endpoint {
        let push_interval_sec = cfg.push_interval_sec.unwrap_or(60);
        runtime_manager.default_runtime.spawn(async move {
            info!("Starting prometheus metrics exporter...");
            loop {
                tokio::time::sleep(Duration::from_secs(push_interval_sec as u64)).await;

                let general_metrics = prometheus::gather();
                let custom_metrics = REGISTRY.gather();
                let mut metrics = vec![];
                metrics.extend_from_slice(&custom_metrics);
                metrics.extend_from_slice(&general_metrics);

                let pushed_result = prometheus::push_metrics(
                    job_name,
                    labels! {"worker_id".to_owned() => worker_uid.to_owned(),},
                    &push_gateway_endpoint.to_owned().unwrap().to_owned(),
                    metrics,
                    None,
                );
                if pushed_result.is_err() {
                    error!("Errors on pushing metrics. {:?}", pushed_result.err());
                }
            }
        });
    }
    return true;
}
