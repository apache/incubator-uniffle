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

pub mod manager;
mod metrics;

use crate::runtime::metrics::Metrics;
use anyhow::anyhow;
use pin_project_lite::pin_project;
use std::fmt::Debug;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::runtime::Runtime as TokioRuntime;
use tokio::task::JoinHandle as TokioJoinHandle;

pub type RuntimeRef = Arc<Runtime>;

#[derive(Debug)]
pub struct Runtime {
    rt: TokioRuntime,
    metrics: Arc<Metrics>,
}

impl Runtime {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle {
            inner: self.rt.spawn(future),
        }
    }

    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        JoinHandle {
            inner: self.rt.spawn_blocking(func),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }

    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            alive_thread_num: self.metrics.thread_alive_gauge.get(),
            idle_thread_num: self.metrics.thread_idle_gauge.get(),
        }
    }
}

#[derive(Debug)]
pub struct RuntimeStats {
    pub alive_thread_num: i64,
    pub idle_thread_num: i64,
}

pin_project! {
    #[derive(Debug)]
    pub struct JoinHandle<T> {
        #[pin]
        inner: TokioJoinHandle<T>,
    }
}

impl<T> JoinHandle<T> {
    pub fn abort(&self) {
        self.inner.abort();
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, anyhow::Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner
            .poll(ctx)
            .map_err(|_source| anyhow!("errors on polling for future."))
    }
}

pub struct Builder {
    thread_name: String,
    builder: TokioRuntimeBuilder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            thread_name: "runtime-worker".to_string(),
            builder: TokioRuntimeBuilder::new_multi_thread(),
        }
    }
}

fn with_metrics<F>(metrics: &Arc<Metrics>, f: F) -> impl Fn()
where
    F: Fn(&Arc<Metrics>) + 'static,
{
    let m = metrics.clone();
    move || {
        f(&m);
    }
}

impl Builder {
    /// Sets the number of worker threads the Runtime will use.
    ///
    /// This can be any number above 0
    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        self.builder.worker_threads(val);
        self
    }

    /// Sets name of threads spawned by the Runtime thread pool
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.thread_name = val.into();
        self
    }

    /// Enable all feature of the underlying runtime
    pub fn enable_all(&mut self) -> &mut Self {
        self.builder.enable_all();
        self
    }

    pub fn build(&mut self) -> anyhow::Result<Runtime> {
        let metrics = Arc::new(Metrics::new(&self.thread_name));

        let rt = self
            .builder
            .thread_name(self.thread_name.clone())
            .on_thread_start(with_metrics(&metrics, |m| {
                m.on_thread_start();
            }))
            .on_thread_stop(with_metrics(&metrics, |m| {
                m.on_thread_stop();
            }))
            .on_thread_park(with_metrics(&metrics, |m| {
                m.on_thread_park();
            }))
            .on_thread_unpark(with_metrics(&metrics, |m| {
                m.on_thread_unpark();
            }))
            .build()?;

        Ok(Runtime { rt, metrics })
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{Builder, Runtime};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn create_runtime(pool_size: usize, name: &str) -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(pool_size as usize)
            .thread_name(name)
            .enable_all()
            .build();
        assert!(runtime.is_ok());
        Arc::new(runtime.unwrap())
    }

    #[test]
    fn test_metrics() {
        let runtime = create_runtime(2usize, "test_metrics");
        thread::sleep(Duration::from_millis(60));

        let stats = runtime.stats();
        assert_eq!(2, stats.idle_thread_num);
        assert_eq!(2, stats.alive_thread_num);

        runtime.spawn(async {
            thread::sleep(Duration::from_millis(1000));
        });

        // waiting the task is invoked.
        thread::sleep(Duration::from_millis(50));

        let stats = runtime.stats();
        assert_eq!(2, stats.alive_thread_num);
        assert_eq!(1, stats.idle_thread_num);
    }

    #[test]
    fn test_nested_spawn() {
        let runtime = create_runtime(4usize, "test_nested_spawn");
        let cloned_rt = runtime.clone();

        let handle = runtime.spawn(async move {
            let mut counter = 0;
            for _ in 0..3 {
                counter += cloned_rt
                    .spawn(async move {
                        thread::sleep(Duration::from_millis(50));
                        1
                    })
                    .await
                    .unwrap()
            }
            counter
        });

        assert_eq!(3, runtime.block_on(handle).unwrap())
    }

    #[test]
    fn test_spawn_block() {
        let runtime = create_runtime(2usize, "test_spawn_block");

        let handle = runtime.spawn(async {
            thread::sleep(Duration::from_millis(50));
            1
        });

        assert_eq!(1, runtime.block_on(handle).unwrap());
    }

    #[test]
    fn test_spawn() {
        let runtime = create_runtime(2usize, "test_spawn");

        let rt_cloned = runtime.clone();

        let res = runtime.block_on(async {
            rt_cloned
                .spawn_blocking(move || {
                    thread::sleep(Duration::from_millis(100));
                    2
                })
                .await
        });
        assert_eq!(res.unwrap(), 2);
    }
}
