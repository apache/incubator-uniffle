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

use crate::app::{
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
    RegisterAppContext, ReleaseBufferContext, RequireBufferContext, WritingViewContext,
};
use crate::await_tree::AWAIT_TREE_REGISTRY;

use crate::config::{Config, HybridStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_IN_SPILL_DATA_SIZE, GAUGE_MEMORY_SPILL_OPERATION, GAUGE_MEMORY_SPILL_TO_HDFS,
    GAUGE_MEMORY_SPILL_TO_LOCALFILE, TOTAL_MEMORY_SPILL_OPERATION,
    TOTAL_MEMORY_SPILL_OPERATION_FAILED, TOTAL_MEMORY_SPILL_TO_HDFS,
    TOTAL_MEMORY_SPILL_TO_LOCALFILE, TOTAL_SPILL_EVENTS_DROPPED,
};
use crate::readable_size::ReadableSize;
#[cfg(feature = "hdfs")]
use crate::store::hdfs::HdfsStore;
use crate::store::localfile::LocalFileStore;
use crate::store::memory::{MemorySnapshot, MemoryStore};

use crate::store::{
    PartitionedDataBlock, Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store,
};
use anyhow::{anyhow, Result};

use async_trait::async_trait;
use log::{debug, error, info, warn};
use prometheus::core::{Atomic, AtomicU64};
use std::any::Any;

use std::collections::VecDeque;

use await_tree::InstrumentAwait;
use spin::mutex::Mutex;
use std::str::FromStr;
use std::sync::Arc;

use crate::runtime::manager::RuntimeManager;
use tokio::sync::Semaphore;

trait PersistentStore: Store + Persistent + Send + Sync {}
impl PersistentStore for LocalFileStore {}

#[cfg(feature = "hdfs")]
impl PersistentStore for HdfsStore {}

const DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY: i32 = 20;

pub struct HybridStore {
    // Box<dyn Store> will build fail
    hot_store: Box<MemoryStore>,

    warm_store: Option<Box<dyn PersistentStore>>,
    cold_store: Option<Box<dyn PersistentStore>>,

    config: HybridStoreConfig,
    memory_spill_lock: Mutex<()>,
    memory_spill_recv: async_channel::Receiver<SpillMessage>,
    memory_spill_send: async_channel::Sender<SpillMessage>,
    memory_spill_event_num: AtomicU64,

    memory_spill_to_cold_threshold_size: Option<u64>,

    memory_spill_max_concurrency: i32,

    memory_watermark_flush_trigger_sender: async_channel::Sender<()>,
    memory_watermark_flush_trigger_recv: async_channel::Receiver<()>,

    runtime_manager: RuntimeManager,
}

#[derive(Clone)]
struct SpillMessage {
    ctx: WritingViewContext,
    id: i64,
    retry_cnt: i32,
    previous_spilled_storage: Option<Arc<Box<dyn PersistentStore>>>,
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    pub fn from(config: Config, runtime_manager: RuntimeManager) -> Self {
        let store_type = &config.store_type.unwrap_or(StorageType::MEMORY);
        if !StorageType::contains_memory(&store_type) {
            panic!("Storage type must contains memory.");
        }

        let mut persistent_stores: VecDeque<Box<dyn PersistentStore>> = VecDeque::with_capacity(2);
        if StorageType::contains_localfile(&store_type) {
            let localfile_store =
                LocalFileStore::from(config.localfile_store.unwrap(), runtime_manager.clone());
            persistent_stores.push_back(Box::new(localfile_store));
        }

        if StorageType::contains_hdfs(&store_type) {
            #[cfg(not(feature = "hdfs"))]
            panic!("The binary is not compiled with feature of hdfs! So the storage type can't involve hdfs.");

            #[cfg(feature = "hdfs")]
            let hdfs_store = HdfsStore::from(config.hdfs_store.unwrap());
            #[cfg(feature = "hdfs")]
            persistent_stores.push_back(Box::new(hdfs_store));
        }

        let (send, recv) = async_channel::unbounded();
        let hybrid_conf = config.hybrid_store.unwrap();
        let memory_spill_to_cold_threshold_size =
            match &hybrid_conf.memory_spill_to_cold_threshold_size {
                Some(v) => Some(ReadableSize::from_str(&v.clone()).unwrap().as_bytes()),
                _ => None,
            };
        let memory_spill_max_concurrency = hybrid_conf
            .memory_spill_max_concurrency
            .unwrap_or(DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY);

        let (watermark_flush_send, watermark_flush_recv) = async_channel::unbounded();

        let store = HybridStore {
            hot_store: Box::new(MemoryStore::from(
                config.memory_store.unwrap(),
                runtime_manager.clone(),
            )),
            warm_store: persistent_stores.pop_front(),
            cold_store: persistent_stores.pop_front(),
            config: hybrid_conf,
            memory_spill_lock: Mutex::new(()),
            memory_spill_recv: recv,
            memory_spill_send: send,
            memory_spill_event_num: AtomicU64::new(0),
            memory_spill_to_cold_threshold_size,
            memory_spill_max_concurrency,
            memory_watermark_flush_trigger_sender: watermark_flush_send,
            memory_watermark_flush_trigger_recv: watermark_flush_recv,
            runtime_manager,
        };
        store
    }

    fn is_memory_only(&self) -> bool {
        self.cold_store.is_none() && self.warm_store.is_none()
    }

    fn is_localfile(&self, store: &dyn Any) -> bool {
        store.is::<LocalFileStore>()
    }

    #[allow(unused)]
    fn is_hdfs(&self, store: &dyn Any) -> bool {
        #[cfg(feature = "hdfs")]
        return store.is::<HdfsStore>();

        #[cfg(not(feature = "hdfs"))]
        false
    }

    async fn memory_spill_to_persistent_store(
        &self,
        spill_message: SpillMessage,
    ) -> Result<String, WorkerError> {
        let mut ctx: WritingViewContext = spill_message.ctx;
        let retry_cnt = spill_message.retry_cnt;

        if retry_cnt > 3 {
            let app_id = ctx.uid.app_id;
            return Err(WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(app_id));
        }

        let blocks = &ctx.data_blocks;
        let mut spill_size = 0i64;
        for block in blocks {
            spill_size += block.length as i64;
        }

        let warm = self
            .warm_store
            .as_ref()
            .ok_or(anyhow!("empty warm store. It should not happen"))?;
        let cold = self.cold_store.as_ref().unwrap_or(warm);

        // we should cover the following cases
        // 1. local store is unhealthy. spill to hdfs
        // 2. event flushed to localfile failed. and exceed retry max cnt, fallback to hdfs
        // 3. huge partition directly flush to hdfs

        // normal assignment
        let mut candidate_store = if warm.is_healthy().await? {
            let cold_spilled_size = self.memory_spill_to_cold_threshold_size.unwrap_or(u64::MAX);
            if cold_spilled_size < spill_size as u64 || ctx.owned_by_huge_partition {
                cold
            } else {
                warm
            }
        } else {
            cold
        };

        // fallback assignment. propose hdfs always is active and stable
        if retry_cnt >= 1 {
            candidate_store = cold;
        }

        match candidate_store.name().await {
            StorageType::LOCALFILE => {
                TOTAL_MEMORY_SPILL_TO_LOCALFILE.inc();
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.inc();
            }
            StorageType::HDFS => {
                TOTAL_MEMORY_SPILL_TO_HDFS.inc();
                GAUGE_MEMORY_SPILL_TO_HDFS.inc();
            }
            _ => {}
        }

        let message = format!(
            "partition uid: {:?}, memory spilled size: {}",
            &ctx.uid, spill_size
        );

        // Resort the blocks by task_attempt_id to support LOCAL ORDER by default.
        // This is for spark AQE.
        ctx.data_blocks.sort_by_key(|block| block.task_attempt_id);

        // when throwing the data lost error, it should fast fail for this partition data.
        let inserted = candidate_store
            .insert(ctx)
            .instrument_await("inserting into the persistent store, invoking [write]")
            .await;
        if let Err(err) = inserted {
            match err {
                WorkerError::PARTIAL_DATA_LOST(msg) => {
                    let err_msg = format!(
                        "Partial data has been lost. Let's abort this partition data. error: {}",
                        &msg
                    );
                    error!("{}", err_msg)
                }
                others => return Err(others),
            }
        }

        match candidate_store.name().await {
            StorageType::LOCALFILE => {
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.dec();
            }
            StorageType::HDFS => {
                GAUGE_MEMORY_SPILL_TO_HDFS.dec();
            }
            _ => {}
        }

        Ok(message)
    }

    pub async fn free_hot_store_allocated_memory_size(&self, size: i64) -> Result<bool> {
        self.hot_store.free_allocated(size).await
    }

    pub async fn get_hot_store_memory_snapshot(&self) -> Result<MemorySnapshot> {
        self.hot_store.memory_snapshot().await
    }

    pub async fn get_hot_store_memory_partitioned_buffer_size(
        &self,
        uid: &PartitionedUId,
    ) -> Result<u64> {
        self.hot_store.get_partitioned_buffer_size(uid).await
    }

    pub fn memory_spill_event_num(&self) -> Result<u64> {
        Ok(self.memory_spill_event_num.get())
    }

    async fn make_memory_buffer_flush(
        &self,
        in_flight_uid: i64,
        blocks: Vec<PartitionedDataBlock>,
        uid: PartitionedUId,
    ) -> Result<()> {
        let writing_ctx = WritingViewContext::from(uid, blocks);

        if self
            .memory_spill_send
            .send(SpillMessage {
                ctx: writing_ctx,
                id: in_flight_uid,
                retry_cnt: 0,
                previous_spilled_storage: None,
            })
            .await
            .is_err()
        {
            error!("Errors on sending spill message to queue. This should not happen.");
        } else {
            self.memory_spill_event_num.inc_by(1);
        }

        Ok(())
    }

    async fn release_data_in_memory(&self, data_size: u64, message: &SpillMessage) -> Result<()> {
        let uid = &message.ctx.uid;
        let in_flight_id = message.id;
        self.hot_store
            .release_in_flight_blocks_in_underlying_staging_buffer(uid.clone(), in_flight_id)
            .await?;
        self.hot_store.free_used(data_size as i64).await?;
        self.hot_store.desc_to_in_flight_buffer_size(data_size);
        Ok(())
    }
}

#[async_trait]
impl Store for HybridStore {
    /// Using the async_channel to keep the immutable self to
    /// the self as the Arc<xxx> rather than mpsc::channel, which
    /// uses the recv(&mut self). I don't hope so.
    fn start(self: Arc<HybridStore>) {
        if self.is_memory_only() {
            return;
        }

        // the handler to accept watermark flush trigger
        let hybrid_store = self.clone();
        self.runtime_manager.default_runtime.spawn(async move {
            let store = hybrid_store.clone();
            while let Ok(_) = &store.memory_watermark_flush_trigger_recv.recv().await {
                if let Err(e) = watermark_flush(store.clone()).await {
                    error!("Errors on handling watermark flush events. err: {:?}", e)
                }
            }
        });

        let await_tree_registry = AWAIT_TREE_REGISTRY.clone();
        let store = self.clone();
        let concurrency_limiter =
            Arc::new(Semaphore::new(store.memory_spill_max_concurrency as usize));
        self.runtime_manager.write_runtime.spawn(async move {
            while let Ok(mut message) = store.memory_spill_recv.recv().await {
                let await_root = await_tree_registry
                    .register(format!("hot->warm flush. uid: {:#?}", &message.ctx.uid))
                    .await;

                // using acquire_owned(), refer to https://github.com/tokio-rs/tokio/issues/1998
                let concurrency_guarder = concurrency_limiter
                    .clone()
                    .acquire_owned()
                    .instrument_await("waiting for the spill concurrent lock.")
                    .await
                    .unwrap();

                TOTAL_MEMORY_SPILL_OPERATION.inc();
                GAUGE_MEMORY_SPILL_OPERATION.inc();
                let store_ref = store.clone();
                store
                    .runtime_manager
                    .write_runtime
                    .spawn(await_root.instrument(async move {
                        let mut size = 0u64;
                        for block in &message.ctx.data_blocks {
                            size += block.length as u64;
                        }

                        GAUGE_IN_SPILL_DATA_SIZE.add(size as i64);
                        match store_ref
                            .memory_spill_to_persistent_store(message.clone())
                            .instrument_await("memory_spill_to_persistent_store.")
                            .await
                        {
                            Ok(msg) => {
                                debug!("{}", msg);
                                if let Err(err) = store_ref.release_data_in_memory(size, &message).await {
                                    error!("Errors on releasing memory data, that should not happen. err: {:#?}", err);
                                }
                            }
                            Err(WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(_)) | Err(WorkerError::PARTIAL_DATA_LOST(_)) => {
                                warn!("Dropping the spill event for app: {:?}. Attention: this will make data lost!", message.ctx.uid.app_id);
                                if let Err(err) = store_ref.release_data_in_memory(size, &message).await {
                                    error!("Errors on releasing memory data when dropping the spill event, that should not happen. err: {:#?}", err);
                                }
                                TOTAL_SPILL_EVENTS_DROPPED.inc();
                            }
                            Err(error) => {
                                TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
                                error!(
                                "Errors on spill memory data to persistent storage for event_id:{:?}. The error: {:#?}",
                                    message.id,
                                    error);

                                message.retry_cnt = message.retry_cnt + 1;
                                // re-push to the queue to execute
                                let _ = store_ref.memory_spill_send.send(message).await;
                            }
                        }
                        store_ref.memory_spill_event_num.dec_by(1);
                        GAUGE_IN_SPILL_DATA_SIZE.sub(size as i64);
                        GAUGE_MEMORY_SPILL_OPERATION.dec();
                        drop(concurrency_guarder);
                    }));
            }
        });
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid.clone();

        let insert_result = self
            .hot_store
            .insert(ctx)
            .instrument_await("inserting data into memory")
            .await;
        if self.is_memory_only() {
            return insert_result;
        }

        if let Some(max_spill_size) = &self.config.memory_single_buffer_max_spill_size {
            // single buffer flush
            if let Ok(size) = ReadableSize::from_str(max_spill_size.as_str()) {
                let buffer = self
                    .hot_store
                    .get_or_create_underlying_staging_buffer(uid.clone());
                let mut buffer_inner = buffer.lock();
                if size.as_bytes() < buffer_inner.get_staging_size()? as u64 {
                    let (in_flight_uid, blocks) = buffer_inner.migrate_staging_to_in_flight()?;
                    self.make_memory_buffer_flush(in_flight_uid, blocks, uid.clone())
                        .await?;
                }
            }
        }

        // if the used size exceed the ratio of high watermark,
        // then send watermark flush trigger
        if let Some(_lock) = self.memory_spill_lock.try_lock() {
            let used_ratio = self.hot_store.memory_used_ratio().await;
            if used_ratio > self.config.memory_spill_high_watermark {
                if let Err(e) = self.memory_watermark_flush_trigger_sender.send(()).await {
                    error!(
                        "Errors on send watermark flush event to handler. err: {:?}",
                        e
                    );
                }
            }
        }

        insert_result
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => {
                self.hot_store.get(ctx).await
            }
            _ => self.warm_store.as_ref().unwrap().get(ctx).await,
        }
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        self.warm_store.as_ref().unwrap().get_index(ctx).await
    }

    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let uid = &ctx.uid.clone();
        self.hot_store
            .require_buffer(ctx)
            .instrument_await(format!("requiring buffers. uid: {:?}", uid))
            .await
    }

    async fn release_buffer(&self, ctx: ReleaseBufferContext) -> Result<i64, WorkerError> {
        self.hot_store.release_buffer(ctx).await
    }

    async fn purge(&self, ctx: PurgeDataContext) -> Result<i64> {
        let app_id = &ctx.app_id;
        let mut removed_size = 0i64;

        removed_size += self.hot_store.purge(ctx.clone()).await?;
        info!("Removed data of app:[{}] in hot store", app_id);
        if self.warm_store.is_some() {
            removed_size += self.warm_store.as_ref().unwrap().purge(ctx.clone()).await?;
            info!("Removed data of app:[{}] in warm store", app_id);
        }
        if self.cold_store.is_some() {
            removed_size += self.cold_store.as_ref().unwrap().purge(ctx.clone()).await?;
            info!("Removed data of app:[{}] in cold store", app_id);
        }
        Ok(removed_size)
    }

    async fn is_healthy(&self) -> Result<bool> {
        async fn check_healthy(store: Option<&Box<dyn PersistentStore>>) -> Result<bool> {
            match store {
                Some(store) => store.is_healthy().await,
                _ => Ok(true),
            }
        }
        let warm = check_healthy(self.warm_store.as_ref())
            .await
            .unwrap_or(false);
        let cold = check_healthy(self.cold_store.as_ref())
            .await
            .unwrap_or(false);
        Ok(self.hot_store.is_healthy().await? && (warm || cold))
    }

    async fn register_app(&self, ctx: RegisterAppContext) -> Result<()> {
        self.hot_store.register_app(ctx.clone()).await?;
        if self.warm_store.is_some() {
            self.warm_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())
                .await?;
        }
        if self.cold_store.is_some() {
            self.cold_store
                .as_ref()
                .unwrap()
                .register_app(ctx.clone())
                .await?;
        }
        Ok(())
    }

    async fn name(&self) -> StorageType {
        unimplemented!()
    }
}

pub async fn watermark_flush(store: Arc<HybridStore>) -> Result<()> {
    let used_ratio = store.hot_store.memory_used_ratio().await;
    if used_ratio <= store.config.memory_spill_high_watermark {
        return Ok(());
    }
    let target_size =
        (store.hot_store.get_capacity()? as f32 * store.config.memory_spill_low_watermark) as i64;
    let buffers = store
        .hot_store
        .get_required_spill_buffer(target_size)
        .instrument_await(format!("getting spill buffers."))
        .await;

    let mut flushed_size = 0u64;
    for (partition_id, buffer) in buffers {
        let mut buffer_inner = buffer.lock();
        let (in_flight_uid, blocks) = buffer_inner.migrate_staging_to_in_flight()?;
        drop(buffer_inner);
        for block in &blocks {
            flushed_size += block.length as u64;
        }
        store
            .make_memory_buffer_flush(in_flight_uid, blocks, partition_id)
            .await?;
    }
    store.hot_store.add_to_in_flight_buffer_size(flushed_size);
    debug!("Trigger spilling in background....");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
    use crate::app::{
        PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext,
        WritingViewContext,
    };
    use crate::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType,
    };

    use crate::store::hybrid::HybridStore;
    use crate::store::ResponseData::Mem;
    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes};

    use std::any::Any;
    use std::collections::VecDeque;

    use std::sync::Arc;
    use std::thread;

    use std::time::Duration;

    #[test]
    fn type_downcast_check() {
        trait Fruit {}

        struct Banana {}
        impl Fruit for Banana {}

        struct Apple {}
        impl Fruit for Apple {}

        fn is_apple(store: &dyn Any) -> bool {
            store.is::<Apple>()
        }

        assert_eq!(true, is_apple(&Apple {}));
        assert_eq!(false, is_apple(&Banana {}));

        let boxed_apple = Box::new(Apple {});
        assert_eq!(true, is_apple(&*boxed_apple));
        assert_eq!(false, is_apple(&boxed_apple));
    }

    #[test]
    fn test_only_memory() {
        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new("20M".to_string()));
        config.hybrid_store = Some(HybridStoreConfig::new(0.8, 0.2, None));
        config.store_type = Some(StorageType::MEMORY);
        let store = HybridStore::from(config, Default::default());

        let runtime = store.runtime_manager.clone();
        assert_eq!(true, runtime.wait(store.is_healthy()).unwrap());
    }

    #[test]
    fn test_vec_pop() {
        let mut stores = VecDeque::with_capacity(2);
        stores.push_back(1);
        stores.push_back(2);
        assert_eq!(1, stores.pop_front().unwrap());
        assert_eq!(2, stores.pop_front().unwrap());
        assert_eq!(None, stores.pop_front());
    }

    fn start_store(
        memory_single_buffer_max_spill_size: Option<String>,
        memory_capacity: String,
    ) -> Arc<HybridStore> {
        let data = b"hello world!";
        let _data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new(memory_capacity));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = Some(HybridStoreConfig::new(
            0.8,
            0.2,
            memory_single_buffer_max_spill_size,
        ));
        config.store_type = Some(StorageType::MEMORY_LOCALFILE);

        // The hybrid store will flush the memory data to file when
        // the data reaches the number of 4
        let store = Arc::new(HybridStore::from(config, Default::default()));
        store
    }

    async fn write_some_data(
        store: Arc<HybridStore>,
        uid: PartitionedUId,
        data_len: i32,
        data: &[u8; 12],
        batch_size: i64,
    ) -> Vec<i64> {
        let mut block_ids = vec![];
        for i in 0..batch_size {
            block_ids.push(i);
            let writing_ctx = WritingViewContext::from(
                uid.clone(),
                vec![PartitionedDataBlock {
                    block_id: i,
                    length: data_len as i32,
                    uncompress_length: 100,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                }],
            );
            let _ = store.insert(writing_ctx).await;
        }

        block_ids
    }

    #[test]
    fn single_buffer_spill_test() -> anyhow::Result<()> {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(
            Some("1".to_string()),
            ((data_len * 10000) as i64).to_string(),
        );
        store.clone().start();

        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        let expected_block_ids = runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            100,
        ));

        thread::sleep(Duration::from_secs(1));

        // read from memory and then from localfile
        let response_data = runtime.wait(store.get(ReadingViewContext {
            uid: uid.clone(),
            reading_options: MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1024 * 1024 * 1024),
            serialized_expected_task_ids_bitmap: Default::default(),
        }))?;

        let mut accepted_block_ids = vec![];
        for segment in response_data.from_memory().shuffle_data_block_segments {
            accepted_block_ids.push(segment.block_id);
        }

        let local_index_data = runtime.wait(store.get_index(ReadingIndexViewContext {
            partition_id: uid.clone(),
        }))?;

        match local_index_data {
            ResponseDataIndex::Local(index) => {
                let mut index_bytes = index.index_data;
                while index_bytes.has_remaining() {
                    // index_bytes_holder.put_i64(next_offset);
                    // index_bytes_holder.put_i32(length);
                    // index_bytes_holder.put_i32(uncompress_len);
                    // index_bytes_holder.put_i64(crc);
                    // index_bytes_holder.put_i64(block_id);
                    // index_bytes_holder.put_i64(task_attempt_id);
                    index_bytes.get_i64();
                    index_bytes.get_i32();
                    index_bytes.get_i32();
                    index_bytes.get_i64();
                    let id = index_bytes.get_i64();
                    index_bytes.get_i64();

                    accepted_block_ids.push(id);
                }
            }
        }

        accepted_block_ids.sort();
        assert_eq!(accepted_block_ids, expected_block_ids);

        Ok(())
    }

    #[tokio::test]
    async fn get_data_from_localfile() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(None, ((data_len * 1) as i64).to_string());
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data, 4).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // case1: all data has been flushed to localfile. the data in memory should be empty
        let last_block_id = -1;
        let reading_view_ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                data_len as i64,
            ),
            serialized_expected_task_ids_bitmap: Default::default(),
        };

        let read_data = store.get(reading_view_ctx).await;
        if read_data.is_err() {
            panic!();
        }
        let read_data = read_data.unwrap();
        match read_data {
            Mem(mem_data) => {
                assert_eq!(0, mem_data.shuffle_data_block_segments.len());
            }
            _ => panic!(),
        }

        // case2: read data from localfile
        // 1. read index file
        // 2. read data
        let index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        match store.get_index(index_view_ctx).await.unwrap() {
            ResponseDataIndex::Local(index) => {
                let mut index_data = index.index_data;
                while index_data.has_remaining() {
                    let offset = index_data.get_i64();
                    let length = index_data.get_i32();
                    let _uncompress = index_data.get_i32();
                    let _crc = index_data.get_i64();
                    let _block_id = index_data.get_i64();
                    let _task_id = index_data.get_i64();

                    let reading_view_ctx = ReadingViewContext {
                        uid: uid.clone(),
                        reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
                        serialized_expected_task_ids_bitmap: None,
                    };
                    println!("reading. offset: {:?}. len: {:?}", offset, length);
                    let read_data = store.get(reading_view_ctx).await.unwrap();
                    match read_data {
                        ResponseData::Local(local_data) => {
                            assert_eq!(Bytes::copy_from_slice(data), local_data.data);
                        }
                        _ => panic!(),
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_localfile_disk_corrupted() {
        // when the local disk is corrupted, the data will be aborted.
        // Anyway, this partition's data should be not reserved on the memory to effect other
        // apps
    }

    #[tokio::test]
    async fn test_localfile_disk_unhealthy() {
        // when the local disk is unhealthy, the data should be flushed
        // to the cold store(like hdfs). If not having cold, it will retry again
        // then again.
    }

    #[test]
    fn test_insert_and_get_from_memory() {
        let data = b"hello world!";
        let data_len = data.len();

        let store = start_store(None, ((data_len * 1) as i64).to_string());
        let runtime = store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };
        runtime.wait(write_some_data(
            store.clone(),
            uid.clone(),
            data_len as i32,
            data,
            4,
        ));
        let mut last_block_id = -1;
        // read data one by one
        for idx in 0..=10 {
            let reading_view_ctx = ReadingViewContext {
                uid: uid.clone(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                    last_block_id,
                    data_len as i64,
                ),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_data = runtime.wait(store.get(reading_view_ctx));
            if read_data.is_err() {
                panic!();
            }

            match read_data.unwrap() {
                Mem(mem_data) => {
                    if idx >= 4 {
                        println!(
                            "idx: {}, len: {}",
                            idx,
                            mem_data.shuffle_data_block_segments.len()
                        );
                        continue;
                    }
                    assert_eq!(Bytes::copy_from_slice(data), mem_data.data);
                    let segments = mem_data.shuffle_data_block_segments;
                    assert_eq!(1, segments.len());
                    last_block_id = segments.get(0).unwrap().block_id;
                }
                _ => panic!(),
            }
        }
    }
}
