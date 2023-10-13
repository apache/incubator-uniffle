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

use crate::config::RuntimeConfig;
use crate::runtime::{Builder, RuntimeRef};
use std::future::Future;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RuntimeManager {
    // for reading data
    pub read_runtime: RuntimeRef,
    // for writing data
    pub write_runtime: RuntimeRef,
    // for grpc service
    pub grpc_runtime: RuntimeRef,
    // for http monitor service
    pub http_runtime: RuntimeRef,
    // the default runtime for not important tasks.
    // like the data purging/ heartbeat / metric push
    pub default_runtime: RuntimeRef,
}

impl Default for RuntimeManager {
    fn default() -> Self {
        RuntimeManager::from(Default::default())
    }
}

impl RuntimeManager {
    pub fn from(config: RuntimeConfig) -> Self {
        fn create_runtime(pool_size: usize, name: &str) -> RuntimeRef {
            Arc::new(
                Builder::default()
                    .worker_threads(pool_size as usize)
                    .thread_name(name)
                    .enable_all()
                    .build()
                    .unwrap(),
            )
        }

        Self {
            read_runtime: create_runtime(config.read_thread_num, "read_thread_pool"),
            write_runtime: create_runtime(config.write_thread_num, "write_thread_pool"),
            grpc_runtime: create_runtime(config.grpc_thread_num, "grpc_thread_pool"),
            http_runtime: create_runtime(config.http_thread_num, "http_thread_pool"),
            default_runtime: create_runtime(config.default_thread_num, "default_thread_pool"),
        }
    }

    // for test cases to wait the future
    pub fn wait<F: Future>(&self, future: F) -> F::Output {
        self.default_runtime.block_on(future)
    }
}
