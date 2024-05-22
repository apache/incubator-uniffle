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

use crate::metric::{
    GAUGE_LOCAL_DISK_CAPACITY, GAUGE_LOCAL_DISK_IS_HEALTHY, GAUGE_LOCAL_DISK_USED,
};
use crate::runtime::manager::RuntimeManager;
use anyhow::{anyhow, Result};
use await_tree::InstrumentAwait;
use bytes::{Bytes, BytesMut};
use futures::AsyncWriteExt;
use log::{error, info, warn};
use opendal::services::Fs;
use opendal::Operator;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;

pub struct LocalDiskConfig {
    pub(crate) high_watermark: f32,
    pub(crate) low_watermark: f32,
    pub(crate) max_concurrency: i32,
}

impl LocalDiskConfig {
    pub fn create_mocked_config() -> Self {
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

pub struct LocalDisk {
    pub(crate) root: String,
    operator: Operator,
    concurrency_limiter: Semaphore,
    is_corrupted: AtomicBool,
    is_healthy: AtomicBool,
    config: LocalDiskConfig,

    capacity: u64,
}

impl LocalDisk {
    pub fn new(
        root: String,
        config: LocalDiskConfig,
        runtime_manager: RuntimeManager,
    ) -> Arc<Self> {
        let mut builder = Fs::default();
        builder.root(&root);
        let operator: Operator = Operator::new(builder).unwrap().finish();

        let disk_capacity =
            Self::get_disk_capacity(&root).expect("Errors on getting disk capacity");

        let instance = LocalDisk {
            root: root.to_string(),
            operator,
            concurrency_limiter: Semaphore::new(config.max_concurrency as usize),
            is_corrupted: AtomicBool::new(false),
            is_healthy: AtomicBool::new(true),
            config,
            capacity: disk_capacity,
        };
        let instance = Arc::new(instance);

        let runtime = runtime_manager.default_runtime.clone();
        let cloned = instance.clone();
        runtime.spawn(async {
            info!("Starting the disk healthy check, root: {}", &cloned.root);
            LocalDisk::loop_check_disk(cloned).await;
        });

        GAUGE_LOCAL_DISK_CAPACITY
            .with_label_values(&[&root])
            .set(disk_capacity as i64);

        instance
    }

    async fn write_read_check(local_disk: Arc<LocalDisk>) -> Result<()> {
        let temp_path = "corruption_check.file";
        // cleanup remaining files before checking.
        local_disk.delete(temp_path).await?;

        let written_data = Bytes::copy_from_slice(b"file corruption check");
        local_disk.write(written_data.clone(), temp_path).await?;
        let read_data = local_disk.read(temp_path, 0, None).await?;
        local_disk.delete(temp_path).await?;

        if written_data != read_data {
            let msg = format!(
                "The local disk has been corrupted. path: {}. expected: {:?}, actual: {:?}",
                &local_disk.root, &written_data, &read_data
            );
            Err(anyhow!(msg))
        } else {
            Ok(())
        }
    }

    async fn loop_check_disk(local_disk: Arc<LocalDisk>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            if local_disk.is_corrupted().unwrap() {
                return;
            }

            let root_ref = &local_disk.root;

            let check_succeed: Result<()> = LocalDisk::write_read_check(local_disk.clone()).await;
            if check_succeed.is_err() {
                local_disk.mark_corrupted();
                GAUGE_LOCAL_DISK_IS_HEALTHY
                    .with_label_values(&[root_ref])
                    .set(1i64);
                error!(
                    "Errors on checking local disk corruption. err: {:#?}",
                    check_succeed.err()
                );
            }

            // get the disk used ratio.
            let disk_capacity = local_disk.capacity;
            let disk_available = Self::get_disk_available(root_ref);
            if disk_available.is_err() {
                error!(
                    "Errors on getting the available of the local disk. err: {:?}",
                    disk_available.err()
                );
                continue;
            }
            let disk_available = disk_available.unwrap();
            let used_ratio = 1.0 - (disk_available as f64 / disk_capacity as f64);

            GAUGE_LOCAL_DISK_USED
                .with_label_values(&[root_ref])
                .set((disk_capacity - disk_available) as i64);

            if local_disk.is_healthy().unwrap()
                && used_ratio > local_disk.config.high_watermark as f64
            {
                warn!("Disk={} has been unhealthy.", &local_disk.root);
                local_disk.mark_unhealthy();
                GAUGE_LOCAL_DISK_IS_HEALTHY
                    .with_label_values(&[root_ref])
                    .set(1i64);
                continue;
            }

            if !local_disk.is_healthy().unwrap()
                && used_ratio < local_disk.config.low_watermark as f64
            {
                warn!("Disk={} has been healthy.", &local_disk.root);
                local_disk.mark_healthy();
                GAUGE_LOCAL_DISK_IS_HEALTHY
                    .with_label_values(&[root_ref])
                    .set(0i64);
                continue;
            }
        }
    }

    pub async fn create_dir(&self, dir: &str) -> Result<()> {
        self.operator.create_dir(dir).await?;
        Ok(())
    }

    // this will ensure the data flushed into the file
    async fn write(&self, data: Bytes, path: &str) -> Result<()> {
        self.operator.write(path, data).await?;
        Ok(())
    }

    pub async fn append(&self, data: Bytes, path: &str) -> Result<()> {
        let _concurrency_guarder = self
            .concurrency_limiter
            .acquire()
            .instrument_await("meet the concurrency limiter")
            .await?;

        let mut writer = self
            .operator
            .writer_with(path)
            .append(true)
            .instrument_await("creating the writer...")
            .await?;
        // we must use the write_all to ensure the buffer consumed by the OS.
        // Please see the detail: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
        writer
            .write_all(&*data)
            .instrument_await("writing the data into buffer...")
            .await?;
        writer
            .flush()
            .instrument_await("committing the data into file...")
            .await?;

        Ok(())
    }

    pub async fn get_file_len(&self, path: &str) -> Result<i64> {
        match self.operator.stat(path).await {
            Ok(meta) => Ok(meta.content_length() as i64),
            Err(_) => Ok(0),
        }
    }

    pub async fn read(&self, path: &str, offset: i64, length: Option<i64>) -> Result<Bytes> {
        if length.is_none() {
            return Ok(Bytes::from(self.operator.read(path).await?));
        }

        let mut reader = self.operator.reader(path).await?;
        reader.seek(SeekFrom::Start(offset as u64)).await?;

        let mut buffer = vec![0; length.unwrap() as usize];
        reader.read_exact(buffer.as_mut()).await?;

        let mut bytes_buffer = BytesMut::new();
        bytes_buffer.extend_from_slice(&*buffer);
        Ok(bytes_buffer.freeze())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.operator.remove_all(path).await?;
        Ok(())
    }

    pub fn mark_corrupted(&self) {
        self.is_corrupted.store(true, Ordering::SeqCst);
    }

    pub fn mark_unhealthy(&self) {
        self.is_healthy.store(false, Ordering::SeqCst);
    }

    pub fn mark_healthy(&self) {
        self.is_healthy.store(true, Ordering::SeqCst);
    }

    pub fn is_corrupted(&self) -> Result<bool> {
        Ok(self.is_corrupted.load(Ordering::SeqCst))
    }

    pub fn is_healthy(&self) -> Result<bool> {
        Ok(self.is_healthy.load(Ordering::SeqCst))
    }

    fn get_disk_used_ratio(root: &str, capacity: u64) -> Result<f64> {
        // Get the total and available space in bytes
        let available_space = fs2::available_space(root)?;
        Ok(1.0 - (available_space as f64 / capacity as f64))
    }

    fn get_disk_capacity(root: &str) -> Result<u64> {
        Ok(fs2::total_space(root)?)
    }

    fn get_disk_available(root: &str) -> Result<u64> {
        Ok(fs2::available_space(root)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::manager::RuntimeManager;
    use crate::store::local::disk::{LocalDisk, LocalDiskConfig};
    use bytes::Bytes;
    use std::time::Duration;

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
        runtime.wait(local_disk.create_dir("a/")).unwrap();
        runtime
            .wait(local_disk.append(Bytes::copy_from_slice(data), "a/b"))
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
            .wait(local_disk.delete("a/"))
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

        awaitility::at_most(Duration::from_secs(10)).until(|| local_disk.is_healthy().unwrap());
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

        runtime.wait(local_disk.create_dir("app-id/")).unwrap();

        for _ in 0..2 {
            let write_result =
                runtime.wait(local_disk.append(Bytes::copy_from_slice(data), relative_path));
            assert!(write_result.is_ok());
        }

        let read_result = runtime.wait(local_disk.read(relative_path, 0, Some(data.len() as i64)));
        assert!(read_result.is_ok());
        let read_data = read_result.unwrap();
        let expected = b"Hello, World!";
        assert_eq!(read_data.as_ref(), expected);

        // read the middle word
        let read_result = runtime.wait(local_disk.read(
            relative_path,
            data.len() as i64,
            Some(data.len() as i64),
        ));
        assert_eq!(read_result.unwrap().as_ref(), expected);

        // read all words
        let read_result = runtime.wait(local_disk.read(relative_path, 0, None));
        let expected = b"Hello, World!Hello, World!";
        assert_eq!(read_result.unwrap().as_ref(), expected);

        temp_dir.close().unwrap();
    }
}
