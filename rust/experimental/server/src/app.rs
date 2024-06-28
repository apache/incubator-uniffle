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

use crate::config::Config;
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_APP_NUMBER, GAUGE_TOPN_APP_RESIDENT_DATA_SIZE, TOTAL_APP_NUMBER,
    TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED, TOTAL_READ_DATA, TOTAL_READ_DATA_FROM_LOCALFILE,
    TOTAL_READ_DATA_FROM_MEMORY, TOTAL_RECEIVED_DATA, TOTAL_REQUIRE_BUFFER_FAILED,
};

use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::hybrid::HybridStore;
use crate::store::memory::MemorySnapshot;
use crate::store::{
    PartitionedDataBlock, RequireBufferResponse, ResponseData, ResponseDataIndex, Store,
    StoreProvider,
};
use crate::util::current_timestamp_sec;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use croaring::treemap::JvmSerializer;
use croaring::Treemap;

use dashmap::DashMap;
use log::{debug, error, info};

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};

use std::hash::{Hash, Hasher};

use std::str::FromStr;

use crate::proto::uniffle::RemoteStorage;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub static SHUFFLE_SERVER_ID: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone)]
pub enum DataDistribution {
    NORMAL,
    #[allow(non_camel_case_types)]
    LOCAL_ORDER,
}

pub const MAX_CONCURRENCY_PER_PARTITION_TO_WRITE: i32 = 20;

#[derive(Debug, Clone)]
pub struct AppConfigOptions {
    pub data_distribution: DataDistribution,
    pub max_concurrency_per_partition_to_write: i32,
    pub remote_storage_config_option: Option<RemoteStorageConfig>,
}

impl AppConfigOptions {
    pub fn new(
        data_distribution: DataDistribution,
        max_concurrency_per_partition_to_write: i32,
        remote_storage_config_option: Option<RemoteStorageConfig>,
    ) -> Self {
        Self {
            data_distribution,
            max_concurrency_per_partition_to_write,
            remote_storage_config_option,
        }
    }
}

impl Default for AppConfigOptions {
    fn default() -> Self {
        AppConfigOptions {
            data_distribution: DataDistribution::LOCAL_ORDER,
            max_concurrency_per_partition_to_write: 20,
            remote_storage_config_option: None,
        }
    }
}

// =============================================================

#[derive(Clone, Debug)]
pub struct RemoteStorageConfig {
    pub root: String,
    pub configs: HashMap<String, String>,
}

impl From<RemoteStorage> for RemoteStorageConfig {
    fn from(remote_conf: RemoteStorage) -> Self {
        let root = remote_conf.path;
        let mut confs = HashMap::new();
        for kv in remote_conf.remote_storage_conf {
            confs.insert(kv.key, kv.value);
        }

        Self {
            root,
            configs: confs,
        }
    }
}

// =============================================================

pub struct App {
    app_id: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, HashSet<i32>>,
    app_config_options: AppConfigOptions,
    latest_heartbeat_time: AtomicU64,
    store: Arc<HybridStore>,
    // key: (shuffle_id, partition_id)
    bitmap_of_blocks: DashMap<(i32, i32), PartitionedMeta>,
    huge_partition_marked_threshold: Option<u64>,
    huge_partition_memory_max_available_size: Option<u64>,

    total_received_data_size: AtomicU64,
    total_resident_data_size: AtomicU64,
}

#[derive(Clone)]
struct PartitionedMeta {
    inner: Arc<RwLock<PartitionedMetaInner>>,
}

struct PartitionedMetaInner {
    blocks_bitmap: Treemap,
    total_size: u64,
}

impl PartitionedMeta {
    fn new() -> Self {
        PartitionedMeta {
            inner: Arc::new(RwLock::new(PartitionedMetaInner {
                blocks_bitmap: Treemap::default(),
                total_size: 0,
            })),
        }
    }

    fn get_data_size(&self) -> Result<u64> {
        let meta = self.inner.read().unwrap();
        Ok(meta.total_size)
    }

    fn incr_data_size(&mut self, data_size: i32) -> Result<()> {
        let mut meta = self.inner.write().unwrap();
        meta.total_size += data_size as u64;
        Ok(())
    }

    fn get_block_ids_bytes(&self) -> Result<Bytes> {
        let meta = self.inner.read().unwrap();
        let serialized_data = meta.blocks_bitmap.serialize()?;
        Ok(Bytes::from(serialized_data))
    }

    fn report_block_ids(&mut self, ids: Vec<i64>) -> Result<()> {
        let mut meta = self.inner.write().unwrap();
        for id in ids {
            meta.blocks_bitmap.add(id as u64);
        }
        Ok(())
    }
}

impl App {
    fn from(
        app_id: String,
        config_options: AppConfigOptions,
        store: Arc<HybridStore>,
        huge_partition_marked_threshold: Option<u64>,
        huge_partition_memory_max_available_size: Option<u64>,
        runtime_manager: RuntimeManager,
    ) -> Self {
        // todo: should throw exception if register failed.
        let copy_app_id = app_id.to_string();
        let app_options = config_options.clone();
        let cloned_store = store.clone();
        let register_result = futures::executor::block_on(async move {
            runtime_manager
                .default_runtime
                .spawn(async move {
                    cloned_store
                        .register_app(RegisterAppContext {
                            app_id: copy_app_id,
                            app_config_options: app_options,
                        })
                        .await
                })
                .await
        });
        if register_result.is_err() {
            error!(
                "Errors on registering app to store: {:#?}",
                register_result.err()
            );
        }

        App {
            app_id,
            partitions: DashMap::new(),
            app_config_options: config_options,
            latest_heartbeat_time: AtomicU64::new(current_timestamp_sec()),
            store,
            bitmap_of_blocks: DashMap::new(),
            huge_partition_marked_threshold,
            huge_partition_memory_max_available_size,
            total_received_data_size: Default::default(),
            total_resident_data_size: Default::default(),
        }
    }

    fn get_latest_heartbeat_time(&self) -> u64 {
        self.latest_heartbeat_time.load(Ordering::SeqCst)
    }

    pub fn heartbeat(&self) -> Result<()> {
        let timestamp = current_timestamp_sec();
        self.latest_heartbeat_time.swap(timestamp, Ordering::SeqCst);
        Ok(())
    }

    pub fn register_shuffle(&self, shuffle_id: i32) -> Result<()> {
        self.partitions
            .entry(shuffle_id)
            .or_insert_with(|| HashSet::new());
        Ok(())
    }

    pub async fn insert(&self, ctx: WritingViewContext) -> Result<i32, WorkerError> {
        let len: i32 = ctx.data_blocks.iter().map(|block| block.length).sum();
        self.get_underlying_partition_bitmap(ctx.uid.clone())
            .incr_data_size(len)?;
        TOTAL_RECEIVED_DATA.inc_by(len as u64);
        self.total_received_data_size.fetch_add(len as u64, SeqCst);
        self.total_resident_data_size.fetch_add(len as u64, SeqCst);

        let ctx = match self.is_huge_partition(&ctx.uid).await {
            Ok(true) => WritingViewContext::new(ctx.uid.clone(), ctx.data_blocks, true),
            _ => ctx,
        };

        self.store.insert(ctx).await?;
        Ok(len)
    }

    pub async fn select(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let response = self.store.get(ctx).await;
        response.map(|data| {
            match &data {
                ResponseData::Local(local_data) => {
                    let length = local_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_LOCALFILE.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
                ResponseData::Mem(mem_data) => {
                    let length = mem_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_MEMORY.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
            };

            data
        })
    }

    pub async fn list_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        self.store.get_index(ctx).await
    }

    async fn is_huge_partition(&self, uid: &PartitionedUId) -> Result<bool> {
        let huge_partition_threshold_option = &self.huge_partition_marked_threshold;
        let huge_partition_memory_used_option = &self.huge_partition_memory_max_available_size;
        if huge_partition_threshold_option.is_none() || huge_partition_memory_used_option.is_none()
        {
            return Ok(false);
        }

        let huge_partition_threshold = &huge_partition_threshold_option.unwrap();

        let meta = self.get_underlying_partition_bitmap(uid.clone());
        let data_size = meta.get_data_size()?;
        if data_size > *huge_partition_threshold {
            return Ok(true);
        }

        return Ok(false);
    }

    async fn is_backpressure_for_huge_partition(&self, uid: &PartitionedUId) -> Result<bool> {
        if !self.is_huge_partition(uid).await? {
            return Ok(false);
        }
        let huge_partition_memory_used = &self.huge_partition_memory_max_available_size;
        let huge_partition_memory = &huge_partition_memory_used.unwrap();

        if self
            .store
            .get_hot_store_memory_partitioned_buffer_size(uid)
            .await?
            > *huge_partition_memory
        {
            info!(
                "[{:?}] with huge partition, it has been writing speed limited",
                uid
            );
            TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.inc();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn free_allocated_memory_size(&self, size: i64) -> Result<bool> {
        self.store.free_hot_store_allocated_memory_size(size).await
    }

    pub async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        if self.is_backpressure_for_huge_partition(&ctx.uid).await? {
            TOTAL_REQUIRE_BUFFER_FAILED.inc();
            return Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION);
        }

        self.store.require_buffer(ctx).await.map_err(|err| {
            TOTAL_REQUIRE_BUFFER_FAILED.inc();
            err
        })
    }

    pub async fn release_buffer(&self, ticket_id: i64) -> Result<i64, WorkerError> {
        self.store
            .release_buffer(ReleaseBufferContext::from(ticket_id))
            .await
    }

    fn get_underlying_partition_bitmap(&self, uid: PartitionedUId) -> PartitionedMeta {
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        let partitioned_meta = self
            .bitmap_of_blocks
            .entry((shuffle_id, partition_id))
            .or_insert_with(|| PartitionedMeta::new());
        partitioned_meta.clone()
    }

    pub fn get_block_ids(&self, ctx: GetBlocksContext) -> Result<Bytes> {
        debug!("get blocks: {:?}", ctx.clone());
        let partitioned_meta = self.get_underlying_partition_bitmap(ctx.uid);
        partitioned_meta.get_block_ids_bytes()
    }

    pub async fn report_block_ids(&self, ctx: ReportBlocksContext) -> Result<()> {
        debug!("Report blocks: {:?}", ctx.clone());
        let mut partitioned_meta = self.get_underlying_partition_bitmap(ctx.uid);
        partitioned_meta.report_block_ids(ctx.blocks)?;

        Ok(())
    }

    pub async fn purge(&self, app_id: String, shuffle_id: Option<i32>) -> Result<()> {
        let removed_size = self
            .store
            .purge(PurgeDataContext::new(app_id, shuffle_id))
            .await?;
        self.total_resident_data_size
            .fetch_sub(removed_size as u64, SeqCst);
        Ok(())
    }

    pub fn total_received_data_size(&self) -> u64 {
        self.total_received_data_size.load(SeqCst)
    }

    pub fn total_resident_data_size(&self) -> u64 {
        self.total_resident_data_size.load(SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct PurgeDataContext {
    pub(crate) app_id: String,
    pub(crate) shuffle_id: Option<i32>,
}

impl PurgeDataContext {
    pub fn new(app_id: String, shuffle_id: Option<i32>) -> PurgeDataContext {
        PurgeDataContext { app_id, shuffle_id }
    }
}

impl From<&str> for PurgeDataContext {
    fn from(app_id_ref: &str) -> Self {
        PurgeDataContext {
            app_id: app_id_ref.to_string(),
            shuffle_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReportBlocksContext {
    pub(crate) uid: PartitionedUId,
    pub(crate) blocks: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct GetBlocksContext {
    pub(crate) uid: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct WritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Vec<PartitionedDataBlock>,
    pub owned_by_huge_partition: bool,
}

impl WritingViewContext {
    pub fn from(uid: PartitionedUId, data_blocks: Vec<PartitionedDataBlock>) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            owned_by_huge_partition: false,
        }
    }

    pub fn new(
        uid: PartitionedUId,
        data_blocks: Vec<PartitionedDataBlock>,
        owned_by_huge_partition: bool,
    ) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            owned_by_huge_partition,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadingViewContext {
    pub uid: PartitionedUId,
    pub reading_options: ReadingOptions,
    pub serialized_expected_task_ids_bitmap: Option<Treemap>,
}

pub struct ReadingIndexViewContext {
    pub partition_id: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct RequireBufferContext {
    pub uid: PartitionedUId,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct RegisterAppContext {
    pub app_id: String,
    pub app_config_options: AppConfigOptions,
}

#[derive(Debug, Clone)]
pub struct ReleaseBufferContext {
    pub(crate) ticket_id: i64,
}

impl From<i64> for ReleaseBufferContext {
    fn from(value: i64) -> Self {
        Self { ticket_id: value }
    }
}

impl RequireBufferContext {
    pub fn new(uid: PartitionedUId, size: i64) -> Self {
        Self { uid, size }
    }
}

#[derive(Debug, Clone)]
pub enum ReadingOptions {
    #[allow(non_camel_case_types)]
    MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(i64, i64),
    #[allow(non_camel_case_types)]
    FILE_OFFSET_AND_LEN(i64, i64),
}

// ==========================================================

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum PurgeEvent {
    // app_id
    HEART_BEAT_TIMEOUT(String),
    // app_id + shuffle_id
    APP_PARTIAL_SHUFFLES_PURGE(String, i32),
    // app_id
    APP_PURGE(String),
}

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    // key: app_id
    apps: DashMap<String, Arc<App>>,
    receiver: async_channel::Receiver<PurgeEvent>,
    sender: async_channel::Sender<PurgeEvent>,
    store: Arc<HybridStore>,
    app_heartbeat_timeout_min: u32,
    config: Config,
    runtime_manager: RuntimeManager,
}

impl AppManager {
    fn new(runtime_manager: RuntimeManager, config: Config) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        let app_heartbeat_timeout_min = config.app_heartbeat_timeout_min.unwrap_or(10);
        let store = Arc::new(StoreProvider::get(runtime_manager.clone(), config.clone()));
        store.clone().start();
        let manager = AppManager {
            apps: DashMap::new(),
            receiver,
            sender,
            store,
            app_heartbeat_timeout_min,
            config,
            runtime_manager: runtime_manager.clone(),
        };
        manager
    }
}

impl AppManager {
    pub fn get_ref(runtime_manager: RuntimeManager, config: Config) -> AppManagerRef {
        let app_ref = Arc::new(AppManager::new(runtime_manager.clone(), config));
        let app_manager_ref_cloned = app_ref.clone();

        runtime_manager.default_runtime.spawn(async move {
            info!("Starting app heartbeat checker...");
            loop {
                // task1: find out heartbeat timeout apps
                tokio::time::sleep(Duration::from_secs(120)).await;

                let _current_timestamp = current_timestamp_sec();
                for item in app_manager_ref_cloned.apps.iter() {
                    let (key, app) = item.pair();
                    let last_time = app.get_latest_heartbeat_time();

                    if current_timestamp_sec() - last_time
                        > (app_manager_ref_cloned.app_heartbeat_timeout_min * 60) as u64
                    {
                        if app_manager_ref_cloned
                            .sender
                            .send(PurgeEvent::HEART_BEAT_TIMEOUT(key.clone()))
                            .await
                            .is_err()
                        {
                            error!(
                                "Errors on sending purge event when app: {} heartbeat timeout",
                                key
                            );
                        }
                    }
                }
            }
        });

        // calculate topN app shuffle data size
        let app_manager_ref = app_ref.clone();
        runtime_manager.default_runtime.spawn(async move {
            info!("Starting calculating topN app shuffle data size...");
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;

                let view = app_manager_ref.apps.clone().into_read_only();
                let mut apps: Vec<_> = view.values().collect();
                apps.sort_by_key(|x| 0 - x.total_resident_data_size());

                let top_n = 10;
                let limit = if apps.len() > top_n {
                    top_n
                } else {
                    apps.len()
                };
                for idx in 0..limit {
                    GAUGE_TOPN_APP_RESIDENT_DATA_SIZE
                        .with_label_values(&[&apps[idx].app_id])
                        .set(apps[idx].total_resident_data_size() as i64);
                }
            }
        });

        let app_manager_cloned = app_ref.clone();
        runtime_manager.default_runtime.spawn(async move {
            info!("Starting purge event handler...");
            while let Ok(event) = app_manager_cloned.receiver.recv().await {
                GAUGE_APP_NUMBER.dec();
                let _ = match event {
                    PurgeEvent::HEART_BEAT_TIMEOUT(app_id) => {
                        info!(
                            "The app:[{}]'s data will be purged due to heartbeat timeout",
                            &app_id
                        );
                        app_manager_cloned.purge_app_data(app_id, None).await
                    }
                    PurgeEvent::APP_PURGE(app_id) => {
                        info!(
                            "The app:[{}] has been finished, its data will be purged.",
                            &app_id
                        );
                        app_manager_cloned.purge_app_data(app_id, None).await
                    }
                    PurgeEvent::APP_PARTIAL_SHUFFLES_PURGE(app_id, shuffle_id) => {
                        info!("The app:[{:?}] with shuffleId: [{:?}] will be purged due to unregister grpc interface", &app_id, shuffle_id);
                        app_manager_cloned.purge_app_data(app_id, Some(shuffle_id)).await
                    }
                }
                .map_err(|err| error!("Errors on purging data. error: {:?}", err));
            }
        });

        app_ref
    }

    pub async fn store_is_healthy(&self) -> Result<bool> {
        self.store.is_healthy().await
    }

    pub async fn store_memory_snapshot(&self) -> Result<MemorySnapshot> {
        self.store.get_hot_store_memory_snapshot().await
    }

    pub fn store_memory_spill_event_num(&self) -> Result<u64> {
        self.store.memory_spill_event_num()
    }

    async fn purge_app_data(&self, app_id: String, shuffle_id_option: Option<i32>) -> Result<()> {
        let app = self.get_app(&app_id).ok_or(anyhow!(format!(
            "App:{} don't exist when purging data, this should not happen",
            &app_id
        )))?;
        app.purge(app_id.clone(), shuffle_id_option).await?;

        if shuffle_id_option.is_none() {
            self.apps.remove(&app_id);
        }

        Ok(())
    }

    pub fn get_app(&self, app_id: &str) -> Option<Arc<App>> {
        self.apps.get(app_id).map(|v| v.value().clone())
    }

    pub fn register(
        &self,
        app_id: String,
        shuffle_id: i32,
        app_config_options: AppConfigOptions,
    ) -> Result<()> {
        info!(
            "Accepted registry. app_id: {}, shuffle_id: {}",
            app_id.clone(),
            shuffle_id
        );
        let app_ref = self.apps.entry(app_id.clone()).or_insert_with(|| {
            TOTAL_APP_NUMBER.inc();
            GAUGE_APP_NUMBER.inc();

            let capacity =
                ReadableSize::from_str(&self.config.memory_store.clone().unwrap().capacity)
                    .unwrap()
                    .as_bytes();
            let huge_partition_max_available_size = Some(
                (self
                    .config
                    .huge_partition_memory_max_used_percent
                    .unwrap_or(1.0)
                    * capacity as f64) as u64,
            );

            let threshold = match &self.config.huge_partition_marked_threshold {
                Some(v) => Some(
                    ReadableSize::from_str(v.clone().as_str())
                        .unwrap()
                        .as_bytes(),
                ),
                _ => None,
            };

            Arc::new(App::from(
                app_id,
                app_config_options,
                self.store.clone(),
                threshold,
                huge_partition_max_available_size,
                self.runtime_manager.clone(),
            ))
        });
        app_ref.register_shuffle(shuffle_id)
    }

    pub async fn unregister_shuffle(&self, app_id: String, shuffle_id: i32) -> Result<()> {
        self.sender
            .send(PurgeEvent::APP_PARTIAL_SHUFFLES_PURGE(app_id, shuffle_id))
            .await?;
        Ok(())
    }

    pub async fn unregister_app(&self, app_id: String) -> Result<()> {
        self.sender.send(PurgeEvent::APP_PURGE(app_id)).await?;
        Ok(())
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedUId {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl PartitionedUId {
    pub fn from(app_id: String, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id,
            shuffle_id,
            partition_id,
        }
    }

    pub fn get_hash(uid: &PartitionedUId) -> u64 {
        let mut hasher = DefaultHasher::new();

        uid.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        AppManager, GetBlocksContext, PartitionedUId, ReadingOptions, ReadingViewContext,
        ReportBlocksContext, WritingViewContext,
    };
    use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig};

    use crate::runtime::manager::RuntimeManager;
    use crate::store::{PartitionedDataBlock, ResponseData};
    use croaring::treemap::JvmSerializer;
    use croaring::Treemap;
    use dashmap::DashMap;

    #[test]
    fn test_uid_hash() {
        let uid = PartitionedUId::from("a".to_string(), 1, 1);
        let hash_value = PartitionedUId::get_hash(&uid);
        println!("{}", hash_value);
    }

    fn mock_config() -> Config {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new((1024 * 1024).to_string()));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = Some(HybridStoreConfig::default());
        config
    }

    #[test]
    fn app_put_get_purge_test() {
        let app_id = "app_put_get_purge_test-----id";

        let runtime_manager: RuntimeManager = Default::default();
        let app_manager_ref = AppManager::get_ref(runtime_manager.clone(), mock_config()).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();

        if let Some(app) = app_manager_ref.get_app("app_id".into()) {
            let writing_ctx = WritingViewContext::from(
                PartitionedUId {
                    app_id: app_id.clone().into(),
                    shuffle_id: 1,
                    partition_id: 0,
                },
                vec![
                    PartitionedDataBlock {
                        block_id: 0,
                        length: 10,
                        uncompress_length: 20,
                        crc: 10,
                        data: Default::default(),
                        task_attempt_id: 0,
                    },
                    PartitionedDataBlock {
                        block_id: 1,
                        length: 20,
                        uncompress_length: 30,
                        crc: 0,
                        data: Default::default(),
                        task_attempt_id: 0,
                    },
                ],
            );

            // case1: put
            let f = app.insert(writing_ctx);
            if runtime_manager.wait(f).is_err() {
                panic!()
            }

            let reading_ctx = ReadingViewContext {
                uid: Default::default(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            // case2: get
            let f = app.select(reading_ctx);
            let result = runtime_manager.wait(f);
            if result.is_err() {
                panic!()
            }

            match result.unwrap() {
                ResponseData::Mem(data) => {
                    assert_eq!(2, data.shuffle_data_block_segments.len());
                }
                _ => todo!(),
            }

            // check the data size
            assert_eq!(30, app.total_received_data_size());
            assert_eq!(30, app.total_resident_data_size());

            // case3: purge
            runtime_manager
                .wait(app_manager_ref.purge_app_data(app_id.to_string(), None))
                .expect("");

            assert_eq!(false, app_manager_ref.get_app(app_id).is_none());

            // check the data size again after the data has been removed
            assert_eq!(30, app.total_received_data_size());
            assert_eq!(0, app.total_resident_data_size());
        }
    }

    #[test]
    fn app_manager_test() {
        let app_manager_ref = AppManager::get_ref(Default::default(), mock_config()).clone();
        app_manager_ref
            .register("app_id".into(), 1, Default::default())
            .unwrap();
        if let Some(app) = app_manager_ref.get_app("app_id".into()) {
            assert_eq!("app_id", app.app_id);
        }
    }

    #[test]
    fn test_get_or_put_block_ids() {
        let app_id = "test_get_or_put_block_ids-----id".to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let app_manager_ref = AppManager::get_ref(runtime_manager.clone(), mock_config()).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();

        let app = app_manager_ref.get_app(app_id.as_ref()).unwrap();
        runtime_manager
            .wait(app.report_block_ids(ReportBlocksContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id: 1,
                    partition_id: 0,
                },
                blocks: vec![123, 124],
            }))
            .expect("TODO: panic message");

        let data = app
            .get_block_ids(GetBlocksContext {
                uid: PartitionedUId {
                    app_id,
                    shuffle_id: 1,
                    partition_id: 0,
                },
            })
            .expect("TODO: panic message");

        let deserialized = Treemap::deserialize(&data).unwrap();
        assert_eq!(deserialized, Treemap::from_iter(vec![123, 124]));
    }

    #[test]
    fn test_dashmap_values() {
        let dashmap = DashMap::new();
        dashmap.insert(1, 3);
        dashmap.insert(2, 2);
        dashmap.insert(3, 8);

        let cloned = dashmap.clone().into_read_only();
        let mut vals: Vec<_> = cloned.values().collect();
        vals.sort_by_key(|x| -(*x));
        assert_eq!(vec![&8, &3, &2], vals);

        let apps = vec![0, 1, 2, 3];
        println!("{:#?}", &apps[0..2]);
    }
}
