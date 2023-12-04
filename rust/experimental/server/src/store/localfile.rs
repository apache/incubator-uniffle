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

use crate::app::ReadingOptions::FILE_OFFSET_AND_LEN;
use crate::app::{
    PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext,
    WritingViewContext,
};
use crate::config::LocalfileStoreConfig;
use crate::error::WorkerError;
use crate::metric::TOTAL_LOCALFILE_USED;
use crate::store::ResponseDataIndex::Local;
use crate::store::{
    LocalDataIndex, PartitionedLocalData, Persistent, RequireBufferResponse, ResponseData,
    ResponseDataIndex, Store,
};

use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;

use log::{debug, error, info, warn};

use crate::runtime::manager::RuntimeManager;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{RwLock, Semaphore};

fn create_directory_if_not_exists(dir_path: &str) {
    if !std::fs::metadata(dir_path).is_ok() {
        std::fs::create_dir_all(dir_path).expect("Errors on creating dirs.");
    }
}

pub struct LocalFileStore {
    local_disks: Vec<Arc<LocalDisk>>,
    partition_written_disk_map: DashMap<String, DashMap<i32, DashMap<i32, Arc<LocalDisk>>>>,
    partition_file_locks: DashMap<String, Arc<RwLock<()>>>,
    healthy_check_min_disks: i32,

    runtime_manager: RuntimeManager,
}

impl Persistent for LocalFileStore {}

unsafe impl Send for LocalFileStore {}
unsafe impl Sync for LocalFileStore {}

impl LocalFileStore {
    // only for test cases
    pub fn new(local_disks: Vec<String>) -> Self {
        let mut local_disk_instances = vec![];
        let runtime_manager: RuntimeManager = Default::default();
        for path in local_disks {
            local_disk_instances.push(LocalDisk::new(
                path,
                LocalDiskConfig::default(),
                runtime_manager.clone(),
            ));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            partition_written_disk_map: DashMap::new(),
            partition_file_locks: DashMap::new(),
            healthy_check_min_disks: 1,
            runtime_manager,
        }
    }

    pub fn from(localfile_config: LocalfileStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let mut local_disk_instances = vec![];
        for path in localfile_config.data_paths {
            let config = LocalDiskConfig {
                high_watermark: localfile_config.disk_high_watermark.unwrap_or(0.8),
                low_watermark: localfile_config.disk_low_watermark.unwrap_or(0.6),
                max_concurrency: localfile_config.disk_max_concurrency.unwrap_or(40),
            };

            local_disk_instances.push(LocalDisk::new(path, config, runtime_manager.clone()));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            partition_written_disk_map: DashMap::new(),
            partition_file_locks: DashMap::new(),
            healthy_check_min_disks: localfile_config.healthy_check_min_disks.unwrap_or(1),
            runtime_manager,
        }
    }

    fn gen_relative_path_for_app(app_id: &str) -> String {
        format!("{}", app_id)
    }

    fn gen_relative_path_for_partition(uid: &PartitionedUId) -> (String, String) {
        (
            format!(
                "{}/{}/partition-{}.data",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
            format!(
                "{}/{}/partition-{}.index",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
        )
    }

    fn get_app_all_partitions(&self, app_id: &str) -> Vec<(i32, i32)> {
        let stage_entry = self.partition_written_disk_map.get(app_id);
        if stage_entry.is_none() {
            return vec![];
        }

        let stages = stage_entry.unwrap();
        let mut partition_ids = vec![];
        for stage_item in stages.iter() {
            let (shuffle_id, partitions) = stage_item.pair();
            for partition_item in partitions.iter() {
                let (partition_id, _) = partition_item.pair();
                partition_ids.push((*shuffle_id, *partition_id));
            }
        }

        partition_ids
    }

    fn delete_app(&self, app_id: &str) -> Result<()> {
        self.partition_written_disk_map.remove(app_id);
        Ok(())
    }

    fn get_owned_disk(&self, uid: PartitionedUId) -> Option<Arc<LocalDisk>> {
        let app_id = uid.app_id;
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;

        let shuffle_entry = self
            .partition_written_disk_map
            .entry(app_id)
            .or_insert_with(|| DashMap::new());
        let partition_entry = shuffle_entry
            .entry(shuffle_id)
            .or_insert_with(|| DashMap::new());

        partition_entry
            .get(&partition_id)
            .map(|v| v.value().clone())
    }

    async fn get_or_create_owned_disk(&self, uid: PartitionedUId) -> Result<Arc<LocalDisk>> {
        let uid_ref = &uid.clone();
        let app_id = uid.app_id;
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;

        let shuffle_entry = self
            .partition_written_disk_map
            .entry(app_id)
            .or_insert_with(|| DashMap::new());
        let partition_entry = shuffle_entry
            .entry(shuffle_id)
            .or_insert_with(|| DashMap::new());
        let local_disk = partition_entry
            .entry(partition_id)
            .or_insert(self.select_disk(uid_ref).await?)
            .clone();

        Ok(local_disk)
    }

    fn healthy_check(&self) -> Result<bool> {
        let mut available = 0;
        for local_disk in &self.local_disks {
            if local_disk.is_healthy()? && !local_disk.is_corrupted()? {
                available += 1;
            }
        }

        debug!(
            "disk: available={}, healthy_check_min={}",
            available, self.healthy_check_min_disks
        );
        Ok(available > self.healthy_check_min_disks)
    }

    async fn select_disk(&self, uid: &PartitionedUId) -> Result<Arc<LocalDisk>, WorkerError> {
        let hash_value = PartitionedUId::get_hash(uid);

        let mut candidates = vec![];
        for local_disk in &self.local_disks {
            if !local_disk.is_corrupted().unwrap() && local_disk.is_healthy().unwrap() {
                candidates.push(local_disk);
            }
        }

        let len = candidates.len();
        if len == 0 {
            error!("There is no available local disk!");
            return Err(WorkerError::NO_AVAILABLE_LOCAL_DISK);
        }

        let index = (hash_value % len as u64) as usize;
        if let Some(&disk) = candidates.get(index) {
            Ok(disk.clone())
        } else {
            Err(WorkerError::INTERNAL_ERROR)
        }
    }
}

#[async_trait]
impl Store for LocalFileStore {
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        if ctx.data_blocks.len() <= 0 {
            return Ok(());
        }

        let uid = ctx.uid;
        let _pid = uid.partition_id;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);
        let local_disk = self.get_or_create_owned_disk(uid.clone()).await?;

        if local_disk.is_corrupted()? {
            return Err(WorkerError::PARTIAL_DATA_LOST(
                local_disk.base_path.to_string(),
            ));
        }

        let lock_cloned = self
            .partition_file_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        let _lock_guard = lock_cloned
            .write()
            .instrument_await(format!(
                "localfile partition file lock. path: {}",
                &data_file_path
            ))
            .await;

        // write index file and data file
        // todo: split multiple pieces
        let mut next_offset = local_disk
            .get_file_len(data_file_path.clone())
            .instrument_await(format!("getting the file len. path: {}", &data_file_path))
            .await?;

        let mut index_bytes_holder = BytesMut::new();
        let mut data_bytes_holder = BytesMut::new();

        let mut total_size = 0;
        for block in ctx.data_blocks {
            let block_id = block.block_id;
            let length = block.length;
            let uncompress_len = block.uncompress_length;
            let task_attempt_id = block.task_attempt_id;
            let crc = block.crc;

            total_size += length;

            index_bytes_holder.put_i64(next_offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = block.data;
            // if get_crc(&data) != crc {
            //     error!("The crc value is not the same. partition id: {}, block id: {}", pid, block_id);
            // }

            data_bytes_holder.extend_from_slice(&data);
            next_offset += length as i64;
        }

        local_disk
            .write(data_bytes_holder.freeze(), data_file_path.clone())
            .instrument_await(format!("localfile writing data. path: {}", data_file_path))
            .await?;
        local_disk
            .write(index_bytes_holder.freeze(), index_file_path.clone())
            .instrument_await(format!("localfile writing index. path: {}", data_file_path))
            .await?;

        TOTAL_LOCALFILE_USED.inc_by(total_size as u64);

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let (offset, len) = match ctx.reading_options {
            FILE_OFFSET_AND_LEN(offset, len) => (offset, len),
            _ => (0, 0),
        };

        if len == 0 {
            warn!("There is no data in localfile for [{:?}]", &uid);
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let (data_file_path, _) = LocalFileStore::gen_relative_path_for_partition(&uid);
        let lock_cloned = self
            .partition_file_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        let _lock_guard = lock_cloned
            .read()
            .instrument_await("getting file read lock")
            .await;

        let local_disk: Option<Arc<LocalDisk>> = self.get_owned_disk(uid.clone());

        if local_disk.is_none() {
            warn!(
                "This should not happen of local disk not found for [{:?}]",
                &uid
            );
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let local_disk = local_disk.unwrap();

        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.base_path.to_string(),
            ));
        }

        let data = local_disk
            .read(data_file_path, offset, Some(len))
            .instrument_await("getting data from localfile")
            .await?;
        Ok(ResponseData::Local(PartitionedLocalData { data }))
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        let uid = ctx.partition_id;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        let lock_cloned = self
            .partition_file_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        let _lock_guard = lock_cloned
            .read()
            .instrument_await("waiting file lock to read index data")
            .await;

        let local_disk: Option<Arc<LocalDisk>> = self.get_owned_disk(uid.clone());

        if local_disk.is_none() {
            warn!(
                "This should not happen of local disk not found for [{:?}]",
                &uid
            );
            return Ok(Local(LocalDataIndex {
                index_data: Default::default(),
                data_file_len: 0,
            }));
        }

        let local_disk = local_disk.unwrap();

        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.base_path.to_string(),
            ));
        }

        let index_data_result = local_disk
            .read(index_file_path, 0, None)
            .instrument_await("reading index data from file")
            .await?;
        let len = local_disk
            .get_file_len(data_file_path)
            .instrument_await("getting file len from file")
            .await?;
        Ok(Local(LocalDataIndex {
            index_data: index_data_result,
            data_file_len: len,
        }))
    }

    async fn require_buffer(
        &self,
        _ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        todo!()
    }

    async fn purge(&self, app_id: String) -> Result<()> {
        let app_relative_dir_path = LocalFileStore::gen_relative_path_for_app(&app_id);

        let all_partition_ids = self.get_app_all_partitions(&app_id);
        if all_partition_ids.is_empty() {
            return Ok(());
        }

        for local_disk_ref in &self.local_disks {
            let disk = local_disk_ref.clone();
            disk.delete(app_relative_dir_path.to_string()).await?;
        }

        for (shuffle_id, partition_id) in all_partition_ids.into_iter() {
            // delete lock
            let uid = PartitionedUId {
                app_id: app_id.clone(),
                shuffle_id,
                partition_id,
            };
            let (data_file_path, _) = LocalFileStore::gen_relative_path_for_partition(&uid);
            self.partition_file_locks.remove(&data_file_path);
        }

        // delete disk mapping
        self.delete_app(&app_id)?;

        Ok(())
    }

    async fn is_healthy(&self) -> Result<bool> {
        self.healthy_check()
    }
}

struct LocalDiskConfig {
    high_watermark: f32,
    low_watermark: f32,
    max_concurrency: i32,
}

impl LocalDiskConfig {
    fn create_mocked_config() -> Self {
        LocalDiskConfig {
            high_watermark: 1.0,
            low_watermark: 0.6,
            max_concurrency: 20,
        }
    }
}

impl Default for LocalDiskConfig {
    fn default() -> Self {
        LocalDiskConfig {
            high_watermark: 0.8,
            low_watermark: 0.6,
            max_concurrency: 40,
        }
    }
}

struct LocalDisk {
    base_path: String,
    concurrency_limiter: Semaphore,
    is_corrupted: AtomicBool,
    is_healthy: AtomicBool,
    config: LocalDiskConfig,
}

impl LocalDisk {
    fn new(path: String, config: LocalDiskConfig, runtime_manager: RuntimeManager) -> Arc<Self> {
        create_directory_if_not_exists(&path);
        let instance = LocalDisk {
            base_path: path,
            concurrency_limiter: Semaphore::new(config.max_concurrency as usize),
            is_corrupted: AtomicBool::new(false),
            is_healthy: AtomicBool::new(true),
            config,
        };
        let instance = Arc::new(instance);

        let runtime = runtime_manager.default_runtime.clone();
        let cloned = instance.clone();
        runtime.spawn(async {
            info!(
                "Starting the disk healthy checking, base path: {}",
                &cloned.base_path
            );
            LocalDisk::loop_check_disk(cloned).await;
        });

        instance
    }

    async fn write_read_check(local_disk: Arc<LocalDisk>) -> Result<()> {
        let temp_path = format!("{}/{}", &local_disk.base_path, "corruption_check.file");
        let data = Bytes::copy_from_slice(b"file corruption check");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&temp_path)
                .await?;
            file.write_all(&data).await?;
            file.flush().await?;
        }

        let mut read_data = Vec::new();
        {
            let mut file = tokio::fs::File::open(&temp_path).await?;
            file.read_to_end(&mut read_data).await?;

            tokio::fs::remove_file(&temp_path).await?;
        }

        if data != Bytes::copy_from_slice(&read_data) {
            local_disk.mark_corrupted();
            error!(
                "The local disk has been corrupted. path: {}",
                &local_disk.base_path
            );
        }

        Ok(())
    }

    async fn loop_check_disk(local_disk: Arc<LocalDisk>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            if local_disk.is_corrupted().unwrap() {
                return;
            }

            let check_succeed: Result<()> = LocalDisk::write_read_check(local_disk.clone()).await;
            if check_succeed.is_err() {
                local_disk.mark_corrupted();
                error!(
                    "Errors on checking local disk corruption. err: {:#?}",
                    check_succeed.err()
                );
            }

            // check the capacity
            let used_ratio = local_disk.get_disk_used_ratio();
            if used_ratio.is_err() {
                error!(
                    "Errors on getting the used ratio of the disk capacity. err: {:?}",
                    used_ratio.err()
                );
                continue;
            }

            let used_ratio = used_ratio.unwrap();
            if local_disk.is_healthy().unwrap()
                && used_ratio > local_disk.config.high_watermark as f64
            {
                warn!("Disk={} has been unhealthy.", &local_disk.base_path);
                local_disk.mark_unhealthy();
                continue;
            }

            if !local_disk.is_healthy().unwrap()
                && used_ratio < local_disk.config.low_watermark as f64
            {
                warn!("Disk={} has been healthy.", &local_disk.base_path);
                local_disk.mark_healthy();
                continue;
            }
        }
    }

    fn append_path(&self, path: String) -> String {
        format!("{}/{}", self.base_path.clone(), path)
    }

    async fn write(&self, data: Bytes, relative_file_path: String) -> Result<()> {
        let _concurrency_guarder = self
            .concurrency_limiter
            .acquire()
            .instrument_await("meet the concurrency limiter")
            .await?;
        let absolute_path = self.append_path(relative_file_path.clone());
        let path = Path::new(&absolute_path);

        match path.parent() {
            Some(parent) => {
                if !parent.exists() {
                    create_directory_if_not_exists(parent.to_str().unwrap())
                }
            }
            _ => todo!(),
        }

        debug!("data file: {}", &absolute_path);

        let mut output_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(absolute_path)
            .await?;
        output_file.write_all(data.as_ref()).await?;
        output_file.flush().await?;

        Ok(())
    }

    async fn get_file_len(&self, relative_file_path: String) -> Result<i64> {
        let file_path = self.append_path(relative_file_path);

        Ok(
            match tokio::fs::metadata(file_path)
                .instrument_await("getting metadata of path")
                .await
            {
                Ok(metadata) => metadata.len() as i64,
                _ => 0i64,
            },
        )
    }

    async fn read(
        &self,
        relative_file_path: String,
        offset: i64,
        length: Option<i64>,
    ) -> Result<Bytes> {
        let file_path = self.append_path(relative_file_path);

        let file = tokio::fs::File::open(&file_path)
            .instrument_await(format!("opening file. path: {}", &file_path))
            .await?;

        let read_len = match length {
            Some(len) => len,
            _ => file
                .metadata()
                .instrument_await(format!("getting file metadata. path: {}", &file_path))
                .await?
                .len()
                .try_into()
                .unwrap(),
        } as usize;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = vec![0; read_len];
        reader
            .seek(SeekFrom::Start(offset as u64))
            .instrument_await(format!(
                "seeking file [{}:{}] of path: {}",
                offset, read_len, &file_path
            ))
            .await?;
        reader
            .read_exact(buffer.as_mut())
            .instrument_await(format!(
                "reading data of len: {} from path: {}",
                read_len, &file_path
            ))
            .await?;

        let mut bytes_buffer = BytesMut::new();
        bytes_buffer.extend_from_slice(&*buffer);
        Ok(bytes_buffer.freeze())
    }

    async fn delete(&self, relative_file_path: String) -> Result<()> {
        let delete_path = self.append_path(relative_file_path);
        if !tokio::fs::try_exists(&delete_path).await? {
            info!("The path:{} does not exist, ignore purging.", &delete_path);
            return Ok(());
        }

        let metadata = tokio::fs::metadata(&delete_path).await?;
        if metadata.is_dir() {
            tokio::fs::remove_dir_all(delete_path).await?;
        } else {
            tokio::fs::remove_file(delete_path).await?;
        }
        Ok(())
    }

    fn mark_corrupted(&self) {
        self.is_corrupted.store(true, Ordering::SeqCst);
    }

    fn mark_unhealthy(&self) {
        self.is_healthy.store(false, Ordering::SeqCst);
    }

    fn mark_healthy(&self) {
        self.is_healthy.store(true, Ordering::SeqCst);
    }

    fn is_corrupted(&self) -> Result<bool> {
        Ok(self.is_corrupted.load(Ordering::SeqCst))
    }

    fn is_healthy(&self) -> Result<bool> {
        Ok(self.is_healthy.load(Ordering::SeqCst))
    }

    fn get_disk_used_ratio(&self) -> Result<f64> {
        // Get the total and available space in bytes
        let available_space = fs2::available_space(&self.base_path)?;
        let total_space = fs2::total_space(&self.base_path)?;
        Ok(1.0 - (available_space as f64 / total_space as f64))
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
        WritingViewContext,
    };
    use crate::store::localfile::{LocalDisk, LocalDiskConfig, LocalFileStore};

    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes, BytesMut};
    use log::info;

    use crate::runtime::manager::RuntimeManager;
    use std::io::Read;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn purge_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.clone()]);

        let runtime = local_store.runtime_manager.clone();

        let app_id = "purge_test-app-id".to_string();
        let uid = PartitionedUId {
            app_id: app_id.clone(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        };

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }
        assert_eq!(
            true,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}/partition-{}.data",
                &temp_path, &app_id, "0", "0"
            )))?
        );
        runtime.wait(local_store.purge(app_id.clone()))?;
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!("{}/{}", &temp_path, &app_id)))?
        );

        Ok(())
    }

    #[test]
    fn local_store_test() {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);
        let mut local_store = LocalFileStore::new(vec![temp_path]);

        let runtime = local_store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        };

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        async fn get_and_check_partitial_data(
            local_store: &mut LocalFileStore,
            uid: PartitionedUId,
            size: i64,
            expected: &[u8],
        ) {
            let reading_ctx = ReadingViewContext {
                uid,
                reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(0, size as i64),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_result = local_store.get(reading_ctx).await;
            if read_result.is_err() {
                panic!()
            }

            match read_result.unwrap() {
                ResponseData::Local(partitioned_data) => {
                    assert_eq!(expected, partitioned_data.data.as_ref());
                }
                _ => panic!(),
            }
        }

        // case1: read the one partition block data
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64,
            data,
        ));

        // case2: read the complete block data
        let mut expected = BytesMut::with_capacity(size * 2);
        expected.extend_from_slice(data);
        expected.extend_from_slice(data);
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64 * 2,
            expected.freeze().as_ref(),
        ));

        // case3: get the index data
        let reading_index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        let result = runtime.wait(local_store.get_index(reading_index_view_ctx));
        if result.is_err() {
            panic!()
        }

        match result.unwrap() {
            ResponseDataIndex::Local(data) => {
                let mut index = data.index_data;
                let offset_1 = index.get_i64();
                assert_eq!(0, offset_1);
                let length_1 = index.get_i32();
                assert_eq!(size as i32, length_1);
                index.get_i32();
                index.get_i64();
                let block_id_1 = index.get_i64();
                assert_eq!(0, block_id_1);
                let task_id = index.get_i64();
                assert_eq!(0, task_id);

                let offset_2 = index.get_i64();
                assert_eq!(size as i64, offset_2);
                assert_eq!(size as i32, index.get_i32());
            }
        }

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_local_disk_delete_operation() {
        let temp_dir = tempdir::TempDir::new("test_local_disk_delete_operation-dir").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        println!("init the path: {}", &temp_path);

        let runtime: RuntimeManager = Default::default();
        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::default(),
            runtime.clone(),
        );

        let data = b"hello!";
        runtime
            .wait(local_disk.write(Bytes::copy_from_slice(data), "a/b".to_string()))
            .unwrap();

        assert_eq!(
            true,
            runtime
                .wait(tokio::fs::try_exists(format!(
                    "{}/{}",
                    &temp_path,
                    "a/b".to_string()
                )))
                .unwrap()
        );

        runtime
            .wait(local_disk.delete("a/".to_string()))
            .expect("TODO: panic message");
        assert_eq!(
            false,
            runtime
                .wait(tokio::fs::try_exists(format!(
                    "{}/{}",
                    &temp_path,
                    "a/b".to_string()
                )))
                .unwrap()
        );
    }

    #[test]
    fn local_disk_corruption_healthy_check() {
        let temp_dir = tempdir::TempDir::new("test_directory").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::create_mocked_config(),
            Default::default(),
        );

        thread::sleep(Duration::from_secs(12));
        assert_eq!(true, local_disk.is_healthy().unwrap());
        assert_eq!(false, local_disk.is_corrupted().unwrap());
    }

    #[test]
    fn local_disk_test() {
        let temp_dir = tempdir::TempDir::new("test_directory").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let runtime: RuntimeManager = Default::default();
        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::default(),
            runtime.clone(),
        );

        let data = b"Hello, World!";

        let relative_path = "app-id/test_file.txt";
        let write_result =
            runtime.wait(local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()));
        assert!(write_result.is_ok());

        // test whether the content is written
        let file_path = format!("{}/{}", local_disk.base_path, relative_path);
        let mut file = std::fs::File::open(file_path).unwrap();
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content).unwrap();
        assert_eq!(file_content, data);

        // if the file has been created, append some content
        let write_result =
            runtime.wait(local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()));
        assert!(write_result.is_ok());

        let read_result = runtime.wait(local_disk.read(
            relative_path.to_string(),
            0,
            Some(data.len() as i64 * 2),
        ));
        assert!(read_result.is_ok());
        let read_data = read_result.unwrap();
        let expected = b"Hello, World!Hello, World!";
        assert_eq!(read_data.as_ref(), expected);

        temp_dir.close().unwrap();
    }
}
