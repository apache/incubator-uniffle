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

use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
use crate::app::{
    PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext,
    WritingViewContext,
};
use crate::config::MemoryStoreConfig;
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_CAPACITY, GAUGE_MEMORY_USED, TOTAL_MEMORY_USED,
};
use crate::readable_size::ReadableSize;
use crate::store::{
    DataSegment, PartitionedDataBlock, PartitionedMemoryData, RequireBufferResponse, ResponseData,
    ResponseDataIndex, Store,
};
use crate::*;
use async_trait::async_trait;
use bytes::BytesMut;
use dashmap::DashMap;

use std::collections::{BTreeMap, HashMap};

use std::str::FromStr;

use crate::store::mem::InstrumentAwait;
use crate::store::mem::MemoryBufferTicket;
use croaring::Treemap;
use log::error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep as delay_for;

pub struct MemoryStore {
    // todo: change to RW lock
    state: DashMap<PartitionedUId, Arc<Mutex<StagingBuffer>>>,
    budget: MemoryBudget,
    // key: app_id, value: allocated memory size
    memory_allocated_of_app: DashMap<String, DashMap<i64, MemoryBufferTicket>>,
    memory_capacity: i64,
    buffer_ticket_timeout_sec: i64,
    buffer_ticket_check_interval_sec: i64,
    in_flush_buffer_size: AtomicU64,
    runtime_manager: RuntimeManager,
}

unsafe impl Send for MemoryStore {}
unsafe impl Sync for MemoryStore {}

impl MemoryStore {
    // only for test cases
    pub fn new(max_memory_size: i64) -> Self {
        MemoryStore {
            state: DashMap::new(),
            budget: MemoryBudget::new(max_memory_size),
            memory_allocated_of_app: DashMap::new(),
            memory_capacity: max_memory_size,
            buffer_ticket_timeout_sec: 5 * 60,
            buffer_ticket_check_interval_sec: 10,
            in_flush_buffer_size: Default::default(),
            runtime_manager: Default::default(),
        }
    }

    pub fn from(conf: MemoryStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let capacity = ReadableSize::from_str(&conf.capacity).unwrap();
        MemoryStore {
            state: DashMap::new(),
            budget: MemoryBudget::new(capacity.as_bytes() as i64),
            memory_allocated_of_app: DashMap::new(),
            memory_capacity: capacity.as_bytes() as i64,
            buffer_ticket_timeout_sec: conf.buffer_ticket_timeout_sec.unwrap_or(5 * 60),
            buffer_ticket_check_interval_sec: 10,
            in_flush_buffer_size: Default::default(),
            runtime_manager,
        }
    }

    // only for tests
    fn refresh_buffer_ticket_check_interval_sec(&mut self, interval: i64) {
        self.buffer_ticket_check_interval_sec = interval
    }

    // todo: make this used size as a var
    pub async fn memory_usage_ratio(&self) -> f32 {
        let snapshot = self.budget.snapshot().await;
        snapshot.get_used_percent()
    }

    pub async fn memory_snapshot(&self) -> Result<MemorySnapshot> {
        Ok(self.budget.snapshot().await)
    }

    pub fn get_capacity(&self) -> Result<i64> {
        Ok(self.memory_capacity)
    }

    pub async fn memory_used_ratio(&self) -> f32 {
        let snapshot = self.budget.snapshot().await;
        (snapshot.used + snapshot.allocated
            - self.in_flush_buffer_size.load(Ordering::SeqCst) as i64) as f32
            / snapshot.capacity as f32
    }

    pub fn add_to_in_flight_buffer_size(&self, size: u64) {
        self.in_flush_buffer_size.fetch_add(size, Ordering::SeqCst);
    }

    pub fn desc_to_in_flight_buffer_size(&self, size: u64) {
        self.in_flush_buffer_size.fetch_sub(size, Ordering::SeqCst);
    }

    pub async fn free_memory(&self, size: i64) -> Result<bool> {
        self.budget.free_used(size).await
    }

    pub async fn get_required_spill_buffer(
        &self,
        target_len: i64,
    ) -> HashMap<PartitionedUId, Arc<Mutex<StagingBuffer>>> {
        // sort
        // get the spill buffers

        let snapshot = self.budget.snapshot().await;
        let removed_size = snapshot.used - target_len;
        if removed_size <= 0 {
            return HashMap::new();
        }

        let mut sorted_tree_map = BTreeMap::new();

        let buffers = self.state.clone().into_read_only();
        for buffer in buffers.iter() {
            let staging_size = buffer.1.lock().await.staging_size;
            let valset = sorted_tree_map
                .entry(staging_size)
                .or_insert_with(|| vec![]);
            let key = buffer.0;
            valset.push(key);
        }

        let mut current_removed = 0;

        let mut required_spill_buffers = HashMap::new();

        let iter = sorted_tree_map.iter().rev();
        'outer: for (size, vals) in iter {
            for pid in vals {
                if current_removed >= removed_size || size.to_be() == 0 {
                    break 'outer;
                }
                current_removed += size.to_be() as i64;
                let partition_uid = (*pid).clone();

                let buffer = self.get_underlying_partition_buffer(&partition_uid);
                required_spill_buffers.insert(partition_uid, buffer);
            }
        }

        required_spill_buffers
    }

    pub async fn get_partitioned_buffer_size(&self, uid: &PartitionedUId) -> Result<u64> {
        let buffer = self.get_underlying_partition_buffer(uid);
        let buffer = buffer.lock().await;
        Ok(buffer.total_size as u64)
    }

    fn get_underlying_partition_buffer(&self, pid: &PartitionedUId) -> Arc<Mutex<StagingBuffer>> {
        self.state.get(pid).unwrap().clone()
    }

    pub async fn release_in_flight_blocks_in_underlying_staging_buffer(
        &self,
        uid: PartitionedUId,
        in_flight_blocks_id: i64,
    ) -> Result<()> {
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let mut buffer_ref = buffer.lock().await;
        buffer_ref.flight_finished(&in_flight_blocks_id)?;
        Ok(())
    }

    pub fn get_or_create_underlying_staging_buffer(
        &self,
        uid: PartitionedUId,
    ) -> Arc<Mutex<StagingBuffer>> {
        let buffer = self
            .state
            .entry(uid)
            .or_insert_with(|| Arc::new(Mutex::new(StagingBuffer::new())));
        buffer.clone()
    }

    pub(crate) fn read_partial_data_with_max_size_limit_and_filter<'a>(
        &'a self,
        blocks: Vec<&'a PartitionedDataBlock>,
        fetched_size_limit: i64,
        serialized_expected_task_ids_bitmap: Option<Treemap>,
    ) -> (Vec<&PartitionedDataBlock>, i64) {
        let mut fetched = vec![];
        let mut fetched_size = 0;

        for block in blocks {
            if let Some(ref filter) = serialized_expected_task_ids_bitmap {
                if !filter.contains(block.task_attempt_id as u64) {
                    continue;
                }
            }
            if fetched_size >= fetched_size_limit {
                break;
            }
            fetched_size += block.length as i64;
            fetched.push(block);
        }

        (fetched, fetched_size)
    }

    pub(crate) fn is_ticket_exist(&self, app_id: &str, ticket_id: i64) -> bool {
        self.memory_allocated_of_app
            .get(app_id)
            .map_or(false, |app_entry| app_entry.contains_key(&ticket_id))
    }

    /// return the discarded memory allocated size
    pub(crate) fn discard_tickets(&self, app_id: &str, ticket_id_option: Option<i64>) -> i64 {
        match ticket_id_option {
            None => self
                .memory_allocated_of_app
                .remove(app_id)
                .map_or(0, |app| app.1.iter().map(|x| x.get_size()).sum()),
            Some(ticket_id) => self.memory_allocated_of_app.get(app_id).map_or(0, |app| {
                app.remove(&ticket_id)
                    .map_or(0, |ticket| ticket.1.get_size())
            }),
        }
    }

    fn cache_buffer_required_ticket(
        &self,
        app_id: &str,
        require_buffer: &RequireBufferResponse,
        size: i64,
    ) {
        let app_entry = self
            .memory_allocated_of_app
            .entry(app_id.to_string())
            .or_insert_with(|| DashMap::default());
        app_entry.insert(
            require_buffer.ticket_id,
            MemoryBufferTicket::new(
                require_buffer.ticket_id,
                require_buffer.allocated_timestamp,
                size,
            ),
        );
    }

    async fn check_allocated_tickets(&self) {
        // if the ticket is timeout, discard this.
        let mut timeout_ids = vec![];
        let iter = self.memory_allocated_of_app.iter();
        for app in iter {
            let app_id = &app.key().to_string();
            let app_iter = app.iter();
            for ticket in app_iter {
                if ticket.is_timeout(self.buffer_ticket_timeout_sec) {
                    timeout_ids.push((app_id.to_string(), ticket.key().clone()))
                }
            }
        }
        for (app_id, ticket_id) in timeout_ids {
            info!(
                "Releasing timeout ticket of id:{}, app_id:{}",
                ticket_id, app_id
            );
            let released = self.discard_tickets(&app_id, Some(ticket_id));
            if let Err(e) = self.budget.free_allocated(released).await {
                error!(
                    "Errors on removing the timeout ticket of id:{}, app_id:{}. error: {:?}",
                    ticket_id, app_id, e
                );
            }
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    fn start(self: Arc<Self>) {
        // schedule check to find out the timeout allocated buffer ticket
        let mem_store = self.clone();
        self.runtime_manager.default_runtime.spawn(async move {
            loop {
                mem_store.check_allocated_tickets().await;
                delay_for(Duration::from_secs(
                    mem_store.buffer_ticket_check_interval_sec as u64,
                ))
                .await;
            }
        });
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer_guarded = buffer
            .lock()
            .instrument_await("trying buffer lock to insert")
            .await;

        let blocks = ctx.data_blocks;
        let inserted_size = buffer_guarded.add(blocks)?;
        drop(buffer_guarded);

        self.budget
            .allocated_to_used(inserted_size)
            .instrument_await("make budget allocated -> used")
            .await?;

        TOTAL_MEMORY_USED.inc_by(inserted_size as u64);

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let buffer = buffer
            .lock()
            .instrument_await("getting partitioned buffer lock")
            .await;

        let options = ctx.reading_options;
        let (fetched_blocks, length) = match options {
            MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, max_size) => {
                let mut last_block_id = last_block_id.clone();
                let mut in_flight_flatten_blocks = vec![];
                for (_, blocks) in buffer.in_flight.iter() {
                    for in_flight_block in blocks {
                        in_flight_flatten_blocks.push(in_flight_block);
                    }
                }
                let in_flight_flatten_blocks = Arc::new(in_flight_flatten_blocks);

                let mut staging_blocks = vec![];
                for block in &buffer.staging {
                    staging_blocks.push(block);
                }
                let staging_blocks = Arc::new(staging_blocks);

                let mut candidate_blocks = vec![];
                // todo: optimize to the way of recursion
                let mut exited = false;
                while !exited {
                    exited = true;
                    if last_block_id == -1 {
                        // Anyway, it will always read from in_flight to staging
                        let mut extends: Vec<&PartitionedDataBlock> = vec![];
                        extends.extend_from_slice(&*in_flight_flatten_blocks);
                        extends.extend_from_slice(&*staging_blocks);
                        candidate_blocks = extends;
                    } else {
                        // check whether the in_fight_blocks exist the last_block_id
                        let mut in_flight_exist = false;
                        let mut unread_in_flight_blocks = vec![];
                        for block in in_flight_flatten_blocks.clone().iter() {
                            if !in_flight_exist {
                                if block.block_id == last_block_id {
                                    in_flight_exist = true;
                                }
                                continue;
                            }
                            unread_in_flight_blocks.push(*block);
                        }

                        if in_flight_exist {
                            let mut extends: Vec<&PartitionedDataBlock> = vec![];
                            extends.extend_from_slice(&*unread_in_flight_blocks);
                            extends.extend_from_slice(&*staging_blocks);
                            candidate_blocks = extends;
                        } else {
                            let mut staging_exist = false;
                            let mut unread_staging_buffers = vec![];
                            for block in staging_blocks.clone().iter() {
                                if !staging_exist {
                                    if block.block_id == last_block_id {
                                        staging_exist = true;
                                    }
                                    continue;
                                }
                                unread_staging_buffers.push(*block);
                            }

                            if staging_exist {
                                candidate_blocks = unread_staging_buffers;
                            } else {
                                // when the last_block_id is not found, maybe the partial has been flush
                                // to file. if having rest data, let's read it from the head
                                exited = false;
                                last_block_id = -1;
                            }
                        }
                    }
                }

                self.read_partial_data_with_max_size_limit_and_filter(
                    candidate_blocks,
                    max_size,
                    ctx.serialized_expected_task_ids_bitmap,
                )
            }
            _ => (vec![], 0),
        };

        let mut bytes_holder = BytesMut::with_capacity(length as usize);
        let mut segments = vec![];
        let mut offset = 0;
        for block in fetched_blocks {
            let data = &block.data;
            bytes_holder.extend_from_slice(data);
            segments.push(DataSegment {
                block_id: block.block_id,
                offset,
                length: block.length,
                uncompress_length: block.uncompress_length,
                crc: block.crc,
                task_attempt_id: block.task_attempt_id,
            });
            offset += block.length as i64;
        }

        Ok(ResponseData::Mem(PartitionedMemoryData {
            shuffle_data_block_segments: segments,
            data: bytes_holder.freeze(),
        }))
    }

    async fn get_index(
        &self,
        _ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        panic!("It should not be invoked.")
    }

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let (succeed, ticket_id) = self.budget.pre_allocate(ctx.size).await?;
        match succeed {
            true => {
                let require_buffer_resp = RequireBufferResponse::new(ticket_id);
                self.cache_buffer_required_ticket(
                    ctx.uid.app_id.as_str(),
                    &require_buffer_resp,
                    ctx.size,
                );
                Ok(require_buffer_resp)
            }
            _ => Err(WorkerError::NO_ENOUGH_MEMORY_TO_BE_ALLOCATED),
        }
    }

    async fn purge(&self, app_id: String) -> Result<()> {
        // free allocated
        let released_size = self.discard_tickets(app_id.as_str(), None);
        self.budget.free_allocated(released_size).await?;
        info!(
            "free allocated buffer size:[{}] for app:[{}]",
            released_size,
            app_id.as_str()
        );

        // remove the corresponding app's data
        let read_only_state_view = self.state.clone().into_read_only();
        let mut _removed_list = vec![];
        for entry in read_only_state_view.iter() {
            let pid = entry.0;
            if pid.app_id == app_id {
                _removed_list.push(pid);
            }
        }

        let mut used = 0;
        for removed_pid in _removed_list {
            if let Some(entry) = self.state.remove(removed_pid) {
                used += entry.1.lock().await.total_size;
            }
        }

        // free used
        self.budget.free_used(used).await?;

        info!(
            "removed used buffer size:[{}] for app:[{}]",
            used,
            app_id.as_str()
        );

        Ok(())
    }

    async fn is_healthy(&self) -> Result<bool> {
        Ok(true)
    }
}

/// thread safe, this will be guarded by the lock
#[derive(Debug, Clone)]
pub struct StagingBuffer {
    // optimize this size.
    total_size: i64,
    staging_size: i64,
    in_flight_size: i64,
    staging: Vec<PartitionedDataBlock>,
    in_flight: BTreeMap<i64, Vec<PartitionedDataBlock>>,
    id_generator: i64,
}

impl StagingBuffer {
    pub fn new() -> StagingBuffer {
        StagingBuffer {
            total_size: 0,
            staging_size: 0,
            in_flight_size: 0,
            staging: vec![],
            in_flight: BTreeMap::new(),
            id_generator: 0,
        }
    }

    /// add the blocks to the staging
    pub fn add(&mut self, blocks: Vec<PartitionedDataBlock>) -> Result<i64> {
        let mut added = 0i64;
        for block in blocks {
            added += block.length as i64;
            self.staging.push(block);
        }
        self.staging_size += added;
        self.total_size += added;
        Ok(added)
    }

    /// make the blocks sent to persistent storage
    pub fn migrate_staging_to_in_flight(&mut self) -> Result<(i64, Vec<PartitionedDataBlock>)> {
        self.in_flight_size += self.staging_size;
        self.staging_size = 0;

        let blocks = self.staging.to_owned();
        self.staging.clear();

        let id = self.id_generator;
        self.id_generator += 1;
        self.in_flight.insert(id.clone(), blocks.clone());

        Ok((id, blocks))
    }

    /// clear the blocks which are flushed to persistent storage
    pub fn flight_finished(&mut self, flight_id: &i64) -> Result<i64> {
        let done = self.in_flight.remove(flight_id);

        let mut removed_size = 0i64;
        if let Some(removed_blocks) = done {
            for removed_block in removed_blocks {
                removed_size += removed_block.length as i64;
            }
        }

        self.total_size -= removed_size;
        self.in_flight_size -= removed_size;

        Ok(removed_size)
    }

    pub fn get_staging_size(&self) -> Result<i64> {
        Ok(self.staging_size)
    }

    pub fn get_in_flight_size(&self) -> Result<i64> {
        Ok(self.in_flight_size)
    }

    pub fn get_total_size(&self) -> Result<i64> {
        Ok(self.total_size)
    }
}

pub struct MemorySnapshot {
    capacity: i64,
    allocated: i64,
    used: i64,
}

impl From<(i64, i64, i64)> for MemorySnapshot {
    fn from(value: (i64, i64, i64)) -> Self {
        MemorySnapshot {
            capacity: value.0,
            allocated: value.1,
            used: value.2,
        }
    }
}

impl MemorySnapshot {
    pub fn get_capacity(&self) -> i64 {
        self.capacity
    }
    pub fn get_allocated(&self) -> i64 {
        self.allocated
    }
    pub fn get_used(&self) -> i64 {
        self.used
    }
    fn get_used_percent(&self) -> f32 {
        (self.allocated + self.used) as f32 / self.capacity as f32
    }
}

#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<std::sync::Mutex<MemoryBudgetInner>>,
}

struct MemoryBudgetInner {
    capacity: i64,
    allocated: i64,
    used: i64,
    allocation_incr_id: i64,
}

impl MemoryBudget {
    fn new(capacity: i64) -> MemoryBudget {
        GAUGE_MEMORY_CAPACITY.set(capacity);
        MemoryBudget {
            inner: Arc::new(std::sync::Mutex::new(MemoryBudgetInner {
                capacity,
                allocated: 0,
                used: 0,
                allocation_incr_id: 0,
            })),
        }
    }

    pub async fn snapshot(&self) -> MemorySnapshot {
        let inner = self.inner.lock().unwrap();
        (inner.capacity, inner.allocated, inner.used).into()
    }

    async fn pre_allocate(&self, size: i64) -> Result<(bool, i64)> {
        let mut inner = self.inner.lock().unwrap();
        let free_space = inner.capacity - inner.allocated - inner.used;
        if free_space < size {
            Ok((false, -1))
        } else {
            inner.allocated += size;
            let now = inner.allocation_incr_id;
            inner.allocation_incr_id += 1;
            GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
            Ok((true, now))
        }
    }

    async fn allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += size;
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    async fn free_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.used < size {
            inner.used = 0;
            // todo: metric
        } else {
            inner.used -= size;
        }
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    async fn free_allocated(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        PartitionedUId, ReadingOptions, ReadingViewContext, RequireBufferContext,
        WritingViewContext,
    };

    use crate::store::memory::MemoryStore;
    use crate::store::ResponseData::Mem;

    use crate::store::{PartitionedDataBlock, PartitionedMemoryData, ResponseData, Store};

    use bytes::BytesMut;
    use core::panic;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::config::MemoryStoreConfig;
    use crate::runtime::manager::RuntimeManager;
    use anyhow::Result;
    use croaring::Treemap;

    #[test]
    fn test_ticket_timeout() -> Result<()> {
        let cfg = MemoryStoreConfig::from("2M".to_string(), 1);
        let runtime_manager: RuntimeManager = Default::default();
        let mut store = MemoryStore::from(cfg, runtime_manager.clone());

        store.refresh_buffer_ticket_check_interval_sec(1);

        let store = Arc::new(store);
        store.clone().start();

        let app_id = "mocked-app-id";
        let ctx = RequireBufferContext::new(PartitionedUId::from(app_id.to_string(), 1, 1), 1000);
        let resp = runtime_manager.wait(store.require_buffer(ctx.clone()))?;
        assert!(store.is_ticket_exist(app_id, resp.ticket_id));

        let snapshot = runtime_manager.wait(store.budget.snapshot());
        assert_eq!(snapshot.allocated, 1000);
        assert_eq!(snapshot.used, 0);

        thread::sleep(Duration::from_secs(5));

        assert!(!store.is_ticket_exist(app_id, resp.ticket_id));

        let snapshot = runtime_manager.wait(store.budget.snapshot());
        assert_eq!(snapshot.allocated, 0);
        assert_eq!(snapshot.used, 0);

        Ok(())
    }

    #[test]
    fn test_memory_buffer_ticket() -> Result<()> {
        let store = MemoryStore::new(1024 * 1000);
        let runtime = store.runtime_manager.clone();

        let app_id = "mocked-app-id";
        let ctx = RequireBufferContext::new(PartitionedUId::from(app_id.to_string(), 1, 1), 1000);
        let resp = runtime.wait(store.require_buffer(ctx.clone()))?;
        let ticket_id_1 = resp.ticket_id;

        let resp = runtime.wait(store.require_buffer(ctx.clone()))?;
        let ticket_id_2 = resp.ticket_id;

        assert!(store.is_ticket_exist(app_id, ticket_id_1));
        assert!(store.is_ticket_exist(app_id, ticket_id_2));
        assert!(!store.is_ticket_exist(app_id, 100239));

        let snapshot = runtime.wait(store.budget.snapshot());
        assert_eq!(snapshot.allocated, 1000 * 2);
        assert_eq!(snapshot.used, 0);

        runtime.wait(store.purge(app_id.to_string()))?;

        let snapshot = runtime.wait(store.budget.snapshot());
        assert_eq!(snapshot.allocated, 0);
        assert_eq!(snapshot.used, 0);

        Ok(())
    }

    #[test]
    fn test_read_buffer_in_flight() {
        let store = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        let writing_view_ctx = create_writing_ctx_with_blocks(10, 10, uid.clone());
        let _ = runtime.wait(store.insert(writing_view_ctx));

        let default_single_read_size = 20;

        // case1: read from -1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            -1,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case2: when the last_block_id doesn't exist, it should return the data like when last_block_id=-1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            100,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case3: read from 3
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            3,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            4,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            5,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case4: some data are in inflight blocks
        let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer = runtime.wait(buffer.lock());
        let owned = buffer.staging.to_owned();
        buffer.staging.clear();
        let mut idx = 0;
        for block in owned {
            buffer.in_flight.insert(idx, vec![block]);
            idx += 1;
        }
        drop(buffer);

        // all data will be fetched from in_flight data
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            3,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            4,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            5,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case5: old data in in_flight and latest data in staging.
        // read it from the block id 9, and read size of 30
        let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer = runtime.wait(buffer.lock());
        buffer.staging.push(PartitionedDataBlock {
            block_id: 20,
            length: 10,
            uncompress_length: 0,
            crc: 0,
            data: BytesMut::with_capacity(10).freeze(),
            task_attempt_id: 0,
        });
        drop(buffer);

        let mem_data = runtime.wait(get_data_with_last_block_id(30, 7, &store, uid.clone()));
        assert_eq!(3, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            8,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            9,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );
        assert_eq!(
            20,
            mem_data
                .shuffle_data_block_segments
                .get(2)
                .unwrap()
                .block_id
        );

        // case6: read the end to return empty result
        let mem_data = runtime.wait(get_data_with_last_block_id(30, 20, &store, uid.clone()));
        assert_eq!(0, mem_data.shuffle_data_block_segments.len());
    }

    async fn get_data_with_last_block_id(
        default_single_read_size: i64,
        last_block_id: i64,
        store: &MemoryStore,
        uid: PartitionedUId,
    ) -> PartitionedMemoryData {
        let ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                default_single_read_size,
            ),
            serialized_expected_task_ids_bitmap: Default::default(),
        };
        if let Ok(data) = store.get(ctx).await {
            match data {
                Mem(mem_data) => mem_data,
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    fn create_writing_ctx_with_blocks(
        _block_number: i32,
        single_block_size: i32,
        uid: PartitionedUId,
    ) -> WritingViewContext {
        let mut data_blocks = vec![];
        for idx in 0..=9 {
            data_blocks.push(PartitionedDataBlock {
                block_id: idx,
                length: single_block_size.clone(),
                uncompress_length: 0,
                crc: 0,
                data: BytesMut::with_capacity(single_block_size as usize).freeze(),
                task_attempt_id: 0,
            });
        }
        WritingViewContext { uid, data_blocks }
    }

    #[test]
    fn test_allocated_and_purge_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let ctx = RequireBufferContext {
            uid: PartitionedUId {
                app_id: "100".to_string(),
                shuffle_id: 0,
                partition_id: 0,
            },
            size: 10000,
        };
        match runtime.default_runtime.block_on(store.require_buffer(ctx)) {
            Ok(_) => {
                let _ = runtime
                    .default_runtime
                    .block_on(store.purge("100".to_string()));
            }
            _ => panic!(),
        }

        let budget = store.budget.inner.lock().unwrap();
        assert_eq!(0, budget.allocated);
        assert_eq!(0, budget.used);
        assert_eq!(1024 * 1024 * 1024, budget.capacity);
    }

    #[test]
    fn test_purge() -> Result<()> {
        let store = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let app_id = "purge_app";
        let shuffle_id = 1;
        let partition = 1;

        let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition);

        // the buffer requested

        let _buffer = runtime
            .wait(store.require_buffer(RequireBufferContext::new(uid.clone(), 40)))
            .expect("");

        let writing_ctx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![PartitionedDataBlock {
                block_id: 0,
                length: 10,
                uncompress_length: 100,
                crc: 99,
                data: Default::default(),
                task_attempt_id: 0,
            }],
        };
        runtime.wait(store.insert(writing_ctx)).expect("");

        let reading_ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };
        let data = runtime.wait(store.get(reading_ctx.clone())).expect("");
        assert_eq!(1, data.from_memory().shuffle_data_block_segments.len());

        // get weak reference to ensure purge can successfully free memory
        let weak_ref_before = store
            .state
            .get(&uid)
            .map(|entry| Arc::downgrade(&entry.value()));
        assert!(
            weak_ref_before.is_some(),
            "Failed to obtain weak reference before purge"
        );

        // purge
        runtime.wait(store.purge(app_id.to_string())).expect("");
        assert!(
            weak_ref_before.clone().unwrap().upgrade().is_none(),
            "Arc should not exist after purge"
        );
        let snapshot = runtime.wait(store.budget.snapshot());
        assert_eq!(snapshot.used, 0);
        // the remaining allocated will be removed.
        assert_eq!(snapshot.allocated, 0);
        assert_eq!(snapshot.capacity, 1024);
        let data = runtime.wait(store.get(reading_ctx.clone())).expect("");
        assert_eq!(0, data.from_memory().shuffle_data_block_segments.len());

        Ok(())
    }

    #[test]
    fn test_put_and_get_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let writing_ctx = WritingViewContext {
            uid: Default::default(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        };
        runtime.wait(store.insert(writing_ctx)).unwrap();

        let reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            ResponseData::Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(data.shuffle_data_block_segments.get(1).unwrap().offset, 10);
            }
            _ => panic!("should not"),
        }
    }

    #[test]
    fn test_block_id_filter_for_memory() {
        let store = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        // 1. insert 2 block
        let writing_ctx = WritingViewContext {
            uid: Default::default(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        };
        runtime.wait(store.insert(writing_ctx)).unwrap();

        // 2. block_ids_filter is empty, should return 2 blocks
        let mut reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            serialized_expected_task_ids_bitmap: Default::default(),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
            }
            _ => panic!("should not"),
        }

        // 3. set serialized_expected_task_ids_bitmap, and set last_block_id equals 1, should return 1 block
        let mut bitmap = Treemap::default();
        bitmap.add(1);
        reading_ctx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(0, 1000000),
            serialized_expected_task_ids_bitmap: Option::from(bitmap.clone()),
        };

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 1);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(
                    data.shuffle_data_block_segments
                        .get(0)
                        .unwrap()
                        .uncompress_length,
                    200
                );
            }
            _ => panic!("should not"),
        }
    }
}
