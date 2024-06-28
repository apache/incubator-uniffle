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

use await_tree::{Registry, TreeRoot};

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type AwaitTreeRegistryRef = Arc<Mutex<Registry<u64>>>;

pub static AWAIT_TREE_REGISTRY: Lazy<AwaitTreeInner> = Lazy::new(|| AwaitTreeInner::new());

#[derive(Clone)]
pub struct AwaitTreeInner {
    inner: AwaitTreeRegistryRef,
    next_id: Arc<AtomicU64>,
}

impl AwaitTreeInner {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Registry::new(await_tree::Config::default()))),
            next_id: Arc::new(Default::default()),
        }
    }

    pub async fn register(&self, msg: String) -> TreeRoot {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = format!("actor=[{}], {}", id, msg);
        self.inner.lock().unwrap().register(id, msg)
    }

    pub fn get_inner(&self) -> AwaitTreeRegistryRef {
        self.inner.clone()
    }
}
