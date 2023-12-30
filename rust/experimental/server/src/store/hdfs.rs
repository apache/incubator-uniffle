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
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingViewContext,
    ReleaseBufferContext, RequireBufferContext, WritingViewContext,
};
use crate::config::HdfsStoreConfig;
use crate::error::WorkerError;

use crate::metric::TOTAL_HDFS_USED;
use crate::store::{Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use anyhow::Result;

use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;

use futures::AsyncWriteExt;
use hdrs::{Client, ClientBuilder};
use log::{error, info};

use std::path::Path;

use std::sync::Arc;
use std::{env, io};
use tokio::sync::{Mutex, Semaphore};

use tracing::debug;

use url::Url;

struct PartitionCachedMeta {
    is_file_created: bool,
    data_len: i64,
}

impl PartitionCachedMeta {
    pub fn reset(&mut self, len: i64) {
        self.data_len = len;
    }
}

impl Default for PartitionCachedMeta {
    fn default() -> Self {
        Self {
            is_file_created: true,
            data_len: 0,
        }
    }
}

pub struct HdfsStore {
    root: String,
    filesystem: Box<Hdrs>,
    concurrency_access_limiter: Semaphore,

    partition_file_locks: DashMap<String, Arc<Mutex<()>>>,
    partition_cached_meta: DashMap<String, PartitionCachedMeta>,
}

unsafe impl Send for HdfsStore {}
unsafe impl Sync for HdfsStore {}
impl Persistent for HdfsStore {}

impl HdfsStore {
    pub fn from(conf: HdfsStoreConfig) -> Self {
        let data_path = conf.data_path;
        let data_url = Url::parse(data_path.as_str()).unwrap();

        let name_node = match data_url.host_str() {
            Some(host) => format!("{}://{}", data_url.scheme(), host),
            _ => "default".to_string(),
        };
        let krb5_cache = env::var("KRB5CACHE_PATH").map_or(None, |v| Some(v));
        let hdfs_user = env::var("HDFS_USER").map_or(None, |v| Some(v));

        let fs = Hdrs::new(name_node.as_str(), krb5_cache, hdfs_user);
        if fs.is_err() {
            error!("Errors on connecting the hdfs. error: {:?}", fs.err());
            panic!();
        }
        let filesystem = fs.unwrap();

        HdfsStore {
            root: data_url.to_string(),
            filesystem: Box::new(filesystem),
            partition_file_locks: DashMap::new(),
            concurrency_access_limiter: Semaphore::new(conf.max_concurrency.unwrap_or(1) as usize),
            partition_cached_meta: Default::default(),
        }
    }

    fn get_app_dir(&self, app_id: &str) -> String {
        format!("{}/{}/", &self.root, app_id)
    }

    fn get_file_path_by_uid(&self, uid: &PartitionedUId) -> (String, String) {
        let app_id = &uid.app_id;
        let shuffle_id = &uid.shuffle_id;
        let p_id = &uid.partition_id;

        (
            format!(
                "{}/{}/{}/{}-{}/partition-{}.data",
                &self.root, app_id, shuffle_id, p_id, p_id, p_id
            ),
            format!(
                "{}/{}/{}/{}-{}/partition-{}.index",
                &self.root, app_id, shuffle_id, p_id, p_id, p_id
            ),
        )
    }
}

#[async_trait]
impl Store for HdfsStore {
    fn start(self: Arc<Self>) {
        info!("There is nothing to do in hdfs store");
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let data_blocks = ctx.data_blocks;

        let (data_file_path, index_file_path) = self.get_file_path_by_uid(&uid);

        let concurrency_guarder = self
            .concurrency_access_limiter
            .acquire()
            .instrument_await(format!(
                "hdfs concurrency limiter. path: {}",
                data_file_path
            ))
            .await
            .map_err(|e| WorkerError::from(e))?;

        let lock_cloned = self
            .partition_file_locks
            .entry(data_file_path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _lock_guard = lock_cloned
            .lock()
            .instrument_await(format!(
                "hdfs partition file lock. path: {}",
                data_file_path
            ))
            .await;

        let mut next_offset = match self.partition_cached_meta.get(&data_file_path) {
            None => {
                // setup the parent folder
                let parent_dir = Path::new(data_file_path.as_str()).parent().unwrap();
                let parent_path_str = format!("{}/", parent_dir.to_str().unwrap());
                debug!("creating dir: {}", parent_path_str.as_str());
                self.filesystem.create_dir(parent_path_str.as_str()).await?;

                // setup the file
                self.filesystem.touch(&data_file_path).await?;
                self.filesystem.touch(&index_file_path).await?;

                self.partition_cached_meta
                    .insert(data_file_path.to_string(), Default::default());
                0
            }
            Some(meta) => meta.data_len,
        };

        let mut index_bytes_holder = BytesMut::new();
        let mut data_bytes_holder = BytesMut::new();

        let mut total_flushed = 0;
        for data_block in data_blocks {
            let block_id = data_block.block_id;
            let crc = data_block.crc;
            let length = data_block.length;
            let task_attempt_id = data_block.task_attempt_id;
            let uncompress_len = data_block.uncompress_length;

            index_bytes_holder.put_i64(next_offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = data_block.data;
            data_bytes_holder.extend_from_slice(&data);

            next_offset += length as i64;

            total_flushed += length;
        }

        self.filesystem
            .append(&data_file_path, data_bytes_holder.freeze())
            .instrument_await(format!("hdfs writing data. path: {}", data_file_path))
            .await?;
        self.filesystem
            .append(&index_file_path, index_bytes_holder.freeze())
            .instrument_await(format!("hdfs writing index. path: {}", data_file_path))
            .await?;

        let mut partition_cached_meta =
            self.partition_cached_meta.get_mut(&data_file_path).unwrap();
        partition_cached_meta.reset(next_offset);

        TOTAL_HDFS_USED.inc_by(total_flushed as u64);

        drop(concurrency_guarder);

        Ok(())
    }

    async fn get(&self, _ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        todo!()
    }

    async fn get_index(
        &self,
        _ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        todo!()
    }

    async fn require_buffer(
        &self,
        _ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        todo!()
    }

    async fn release_buffer(&self, _ctx: ReleaseBufferContext) -> Result<i64, WorkerError> {
        todo!()
    }

    async fn purge(&self, ctx: PurgeDataContext) -> Result<()> {
        let app_id = ctx.app_id;
        let app_dir = self.get_app_dir(app_id.as_str());

        let keys_to_delete: Vec<_> = self
            .partition_file_locks
            .iter()
            .filter(|entry| entry.key().contains(app_dir.as_str()))
            .map(|entry| entry.key().to_string())
            .collect();

        for deleted_key in keys_to_delete {
            self.partition_file_locks.remove(&deleted_key);
            self.partition_cached_meta.remove(&deleted_key);
        }

        info!("The hdfs data for {} has been deleted", &app_dir);
        self.filesystem.delete_dir(app_dir.as_str()).await
    }

    async fn is_healthy(&self) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
trait HdfsDelegator {
    async fn touch(&self, file_path: &str) -> Result<()>;
    async fn append(&self, file_path: &str, data: Bytes) -> Result<()>;
    async fn len(&self, file_path: &str) -> Result<u64>;

    async fn create_dir(&self, dir: &str) -> Result<()>;
    async fn delete_dir(&self, dir: &str) -> Result<()>;
}

struct Hdrs {
    client: Client,
}

#[async_trait]
impl HdfsDelegator for Hdrs {
    async fn touch(&self, file_path: &str) -> Result<()> {
        let metadata = self.client.metadata(file_path);
        if metadata.is_err() && metadata.unwrap_err().kind() == io::ErrorKind::NotFound {
            debug!("Creating the file, path: {}", file_path);
            let mut write = self
                .client
                .open_file()
                .create(true)
                .write(true)
                .async_open(file_path)
                .await?;
            write.write("".as_bytes()).await?;
            write.flush().await?;
            write.close().await?;
            debug!("the file: {} is created!", file_path);
        }
        Ok(())
    }

    async fn append(&self, file_path: &str, data: Bytes) -> Result<()> {
        let mut data_writer = self
            .client
            .open_file()
            .create(true)
            .append(true)
            .async_open(file_path)
            .await?;
        data_writer.write_all(data.as_ref()).await?;
        data_writer.flush().await?;
        data_writer.close().await?;
        debug!("data has been flushed. path: {}", file_path);
        Ok(())
    }

    async fn create_dir(&self, dir: &str) -> Result<()> {
        self.client.create_dir(dir)?;
        Ok(())
    }

    async fn len(&self, file_path: &str) -> Result<u64> {
        let meta = self.client.metadata(file_path)?;
        Ok(meta.len())
    }

    async fn delete_dir(&self, dir: &str) -> Result<()> {
        self.client.remove_dir_all(dir)?;
        Ok(())
    }
}

impl Hdrs {
    fn new(name_node: &str, krb5_cache: Option<String>, user: Option<String>) -> Result<Self> {
        let mut builder = ClientBuilder::new(name_node);
        if krb5_cache.is_some() {
            builder = builder.with_kerberos_ticket_cache_path(krb5_cache.unwrap().as_str());
        }
        if user.is_some() {
            builder = builder.with_user(user.unwrap().as_str())
        }
        let client = builder.connect()?;
        Ok(Hdrs { client })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn dir_test() -> anyhow::Result<()> {
        let file_path = "app/0/1.data";
        let parent_path = Path::new(file_path).parent().unwrap();
        println!("{}", parent_path.to_str().unwrap());

        Ok(())
    }
}
