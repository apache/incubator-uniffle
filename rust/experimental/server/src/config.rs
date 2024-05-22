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

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryStoreConfig {
    pub capacity: String,
    pub buffer_ticket_timeout_sec: Option<i64>,
}

impl MemoryStoreConfig {
    pub fn new(capacity: String) -> Self {
        Self {
            capacity,
            buffer_ticket_timeout_sec: Some(5 * 60),
        }
    }

    pub fn from(capacity: String, buffer_ticket_timeout_sec: i64) -> Self {
        Self {
            capacity,
            buffer_ticket_timeout_sec: Some(buffer_ticket_timeout_sec),
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct HdfsStoreConfig {
    pub max_concurrency: Option<i32>,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LocalfileStoreConfig {
    pub data_paths: Vec<String>,
    pub healthy_check_min_disks: Option<i32>,
    pub disk_high_watermark: Option<f32>,
    pub disk_low_watermark: Option<f32>,
    pub disk_max_concurrency: Option<i32>,
}

impl LocalfileStoreConfig {
    pub fn new(data_paths: Vec<String>) -> Self {
        LocalfileStoreConfig {
            data_paths,
            healthy_check_min_disks: None,
            disk_high_watermark: None,
            disk_low_watermark: None,
            disk_max_concurrency: None,
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RuntimeConfig {
    pub read_thread_num: usize,
    pub write_thread_num: usize,
    pub grpc_thread_num: usize,
    pub http_thread_num: usize,
    pub default_thread_num: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            read_thread_num: 10,
            write_thread_num: 40,
            grpc_thread_num: 100,
            http_thread_num: 5,
            default_thread_num: 5,
        }
    }
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HybridStoreConfig {
    pub memory_spill_high_watermark: f32,
    pub memory_spill_low_watermark: f32,
    pub memory_single_buffer_max_spill_size: Option<String>,
    pub memory_spill_to_cold_threshold_size: Option<String>,

    pub memory_spill_max_concurrency: Option<i32>,
}

impl HybridStoreConfig {
    pub fn new(
        memory_spill_high_watermark: f32,
        memory_spill_low_watermark: f32,
        memory_single_buffer_max_spill_size: Option<String>,
    ) -> Self {
        HybridStoreConfig {
            memory_spill_high_watermark,
            memory_spill_low_watermark,
            memory_single_buffer_max_spill_size,
            memory_spill_to_cold_threshold_size: None,
            memory_spill_max_concurrency: None,
        }
    }
}

impl Default for HybridStoreConfig {
    fn default() -> Self {
        HybridStoreConfig {
            memory_spill_high_watermark: 0.8,
            memory_spill_low_watermark: 0.7,
            memory_single_buffer_max_spill_size: None,
            memory_spill_to_cold_threshold_size: None,
            memory_spill_max_concurrency: None,
        }
    }
}

fn as_default_runtime_config() -> RuntimeConfig {
    RuntimeConfig::default()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Config {
    pub memory_store: Option<MemoryStoreConfig>,
    pub localfile_store: Option<LocalfileStoreConfig>,
    pub hybrid_store: Option<HybridStoreConfig>,
    pub hdfs_store: Option<HdfsStoreConfig>,

    pub store_type: Option<StorageType>,

    #[serde(default = "as_default_runtime_config")]
    pub runtime_config: RuntimeConfig,

    pub metrics: Option<MetricsConfig>,

    pub grpc_port: Option<i32>,
    pub coordinator_quorum: Vec<String>,
    pub tags: Option<Vec<String>>,

    pub log: Option<LogConfig>,

    pub app_heartbeat_timeout_min: Option<u32>,

    pub huge_partition_marked_threshold: Option<String>,
    pub huge_partition_memory_max_used_percent: Option<f64>,

    pub http_monitor_service_port: Option<u16>,
}

// =========================================================
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetricsConfig {
    pub push_gateway_endpoint: Option<String>,
    pub push_interval_sec: Option<u32>,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogConfig {
    pub path: String,
    pub rotation: RotationConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            path: "/tmp/".to_string(),
            rotation: RotationConfig::Hourly,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RotationConfig {
    Hourly,
    Daily,
    Never,
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Copy)]
#[allow(non_camel_case_types)]
pub enum StorageType {
    MEMORY = 1,
    LOCALFILE = 2,
    MEMORY_LOCALFILE = 3,
    HDFS = 4,
    MEMORY_HDFS = 5,
    MEMORY_LOCALFILE_HDFS = 7,
}

impl StorageType {
    pub fn contains_localfile(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::LOCALFILE as u8 != 0
    }

    pub fn contains_memory(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::MEMORY as u8 != 0
    }

    pub fn contains_hdfs(storage_type: &StorageType) -> bool {
        let val = *storage_type as u8;
        val & *&StorageType::HDFS as u8 != 0
    }
}

const CONFIG_FILE_PATH_KEY: &str = "WORKER_CONFIG_PATH";

impl Config {
    pub fn from(cfg_path: &str) -> Self {
        let path = Path::new(cfg_path);

        // Read the file content as a string
        let file_content = fs::read_to_string(path).expect("Failed to read file");

        toml::from_str(&file_content).unwrap()
    }

    pub fn create_from_env() -> Config {
        let path = match std::env::var(CONFIG_FILE_PATH_KEY) {
            Ok(val) => val,
            _ => panic!(
                "config path must be set in env args. key: {}",
                CONFIG_FILE_PATH_KEY
            ),
        };

        Config::from(&path)
    }
}

#[cfg(test)]
mod test {
    use crate::config::{Config, RuntimeConfig, StorageType};
    use crate::readable_size::ReadableSize;
    use std::str::FromStr;

    #[test]
    fn storage_type_test() {
        let stype = StorageType::MEMORY_LOCALFILE;
        assert_eq!(true, StorageType::contains_localfile(&stype));

        let stype = StorageType::MEMORY_LOCALFILE;
        assert_eq!(true, StorageType::contains_memory(&stype));
        assert_eq!(false, StorageType::contains_hdfs(&stype));

        let stype = StorageType::MEMORY_LOCALFILE_HDFS;
        assert_eq!(true, StorageType::contains_hdfs(&stype));
    }

    #[test]
    fn config_test() {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024M"

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_spill_high_watermark = 0.8
        memory_spill_low_watermark = 0.2
        memory_single_buffer_max_spill_size = "256M"
        "#;

        let decoded: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", decoded);

        let capacity = ReadableSize::from_str(&decoded.memory_store.unwrap().capacity).unwrap();
        assert_eq!(1024 * 1024 * 1024, capacity.as_bytes());

        assert_eq!(
            decoded.runtime_config.read_thread_num,
            RuntimeConfig::default().read_thread_num
        );
    }
}
