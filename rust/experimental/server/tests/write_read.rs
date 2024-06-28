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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use uniffle_worker::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, MetricsConfig,
        StorageType,
    };
    use uniffle_worker::proto::uniffle::shuffle_server_client::ShuffleServerClient;
    use uniffle_worker::{start_uniffle_worker, write_read_for_one_time};

    use std::time::Duration;
    use tonic::transport::Channel;
    use uniffle_worker::metric::GAUGE_MEMORY_ALLOCATED;

    fn create_mocked_config(grpc_port: i32, capacity: String, local_data_path: String) -> Config {
        Config {
            memory_store: Some(MemoryStoreConfig::new(capacity)),
            localfile_store: Some(LocalfileStoreConfig {
                data_paths: vec![local_data_path],
                healthy_check_min_disks: Some(0),
                disk_high_watermark: None,
                disk_low_watermark: None,
                disk_max_concurrency: None,
            }),
            hybrid_store: Some(HybridStoreConfig::new(0.9, 0.5, None)),
            hdfs_store: None,
            store_type: Some(StorageType::MEMORY_LOCALFILE),
            runtime_config: Default::default(),
            metrics: Some(MetricsConfig {
                push_gateway_endpoint: None,
                push_interval_sec: None,
            }),
            grpc_port: Some(grpc_port),
            coordinator_quorum: vec![],
            tags: None,
            log: None,
            app_heartbeat_timeout_min: None,
            huge_partition_marked_threshold: None,
            huge_partition_memory_max_used_percent: None,
            http_monitor_service_port: None,
        }
    }

    async fn get_data_from_remote(
        _client: &ShuffleServerClient<Channel>,
        _app_id: &str,
        _shuffle_id: i32,
        _partitions: Vec<i32>,
    ) {
    }

    async fn start_embedded_worker(path: String, port: i32) {
        let config = create_mocked_config(port, "1G".to_string(), path);
        let _ = start_uniffle_worker(config).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_read_test_with_embedded_worker() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let port = 21101;
        let _ = start_embedded_worker(temp_path, port).await;

        let client = ShuffleServerClient::connect(format!("http://{}:{}", "0.0.0.0", port)).await?;

        // after one batch write/read process, the allocated memory size should be 0
        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());

        write_read_for_one_time(client).await
    }
}
