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

#[cfg(feature = "hdfs")]
pub mod hdfs;
pub mod hybrid;
pub mod local;
pub mod localfile;
pub mod mem;
pub mod memory;

use crate::app::{
    PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
    ReleaseBufferContext, RequireBufferContext, WritingViewContext,
};
use crate::config::{Config, StorageType};
use crate::error::WorkerError;
use crate::proto::uniffle::{ShuffleData, ShuffleDataBlockSegment};
use crate::store::hybrid::HybridStore;

use crate::util::current_timestamp_sec;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

use crate::runtime::manager::RuntimeManager;
use std::sync::Arc;

#[derive(Debug)]
pub struct PartitionedData {
    pub partition_id: i32,
    pub blocks: Vec<PartitionedDataBlock>,
}

#[derive(Debug, Clone)]
pub struct PartitionedDataBlock {
    pub block_id: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub data: Bytes,
    pub task_attempt_id: i64,
}

impl From<ShuffleData> for PartitionedData {
    fn from(shuffle_data: ShuffleData) -> PartitionedData {
        let mut blocks = vec![];
        for data in shuffle_data.block {
            let block = PartitionedDataBlock {
                block_id: data.block_id,
                length: data.length,
                uncompress_length: data.uncompress_length,
                crc: data.crc,
                data: data.data,
                task_attempt_id: data.task_attempt_id,
            };
            blocks.push(block);
        }
        PartitionedData {
            partition_id: shuffle_data.partition_id,
            blocks,
        }
    }
}

pub enum ResponseDataIndex {
    Local(LocalDataIndex),
}

pub struct LocalDataIndex {
    pub index_data: Bytes,
    pub data_file_len: i64,
}

pub enum ResponseData {
    Local(PartitionedLocalData),
    Mem(PartitionedMemoryData),
}

impl ResponseData {
    pub fn from_local(&self) -> Bytes {
        match self {
            ResponseData::Local(data) => data.data.clone(),
            _ => Default::default(),
        }
    }

    pub fn from_memory(&self) -> PartitionedMemoryData {
        match self {
            ResponseData::Mem(data) => data.clone(),
            _ => Default::default(),
        }
    }
}

pub struct PartitionedLocalData {
    pub data: Bytes,
}

#[derive(Clone, Default)]
pub struct PartitionedMemoryData {
    pub shuffle_data_block_segments: Vec<DataSegment>,
    pub data: Bytes,
}

#[derive(Clone)]
pub struct DataSegment {
    pub block_id: i64,
    pub offset: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub task_attempt_id: i64,
}

impl Into<ShuffleDataBlockSegment> for DataSegment {
    fn into(self) -> ShuffleDataBlockSegment {
        ShuffleDataBlockSegment {
            block_id: self.block_id,
            offset: self.offset,
            length: self.length,
            uncompress_length: self.uncompress_length,
            crc: self.crc,
            task_attempt_id: self.task_attempt_id,
        }
    }
}

// =====================================================

#[derive(Clone, Debug)]
pub struct RequireBufferResponse {
    pub ticket_id: i64,
    pub allocated_timestamp: u64,
}

impl RequireBufferResponse {
    fn new(ticket_id: i64) -> Self {
        Self {
            ticket_id,
            allocated_timestamp: current_timestamp_sec(),
        }
    }
}

// =====================================================

#[async_trait]
pub trait Store {
    fn start(self: Arc<Self>);
    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError>;
    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError>;
    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError>;
    async fn purge(&self, ctx: PurgeDataContext) -> Result<i64>;
    async fn is_healthy(&self) -> Result<bool>;

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError>;
    async fn release_buffer(&self, ctx: ReleaseBufferContext) -> Result<i64, WorkerError>;
    async fn register_app(&self, ctx: RegisterAppContext) -> Result<()>;

    async fn name(&self) -> StorageType;
}

pub trait Persistent {}

pub struct StoreProvider {}

impl StoreProvider {
    pub fn get(runtime_manager: RuntimeManager, config: Config) -> HybridStore {
        HybridStore::from(config, runtime_manager)
    }
}
