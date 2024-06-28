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

use crate::metric::{GAUGE_RUNTIME_ALIVE_THREAD_NUM, GAUGE_RUNTIME_IDLE_THREAD_NUM};
use prometheus::IntGauge;

#[derive(Debug)]
pub struct Metrics {
    pub thread_alive_gauge: IntGauge,
    pub thread_idle_gauge: IntGauge,
}

impl Metrics {
    pub fn new(name: &str) -> Self {
        Self {
            thread_alive_gauge: GAUGE_RUNTIME_ALIVE_THREAD_NUM.with_label_values(&[name]),
            thread_idle_gauge: GAUGE_RUNTIME_IDLE_THREAD_NUM.with_label_values(&[name]),
        }
    }

    #[inline]
    pub fn on_thread_start(&self) {
        self.thread_alive_gauge.inc();
    }

    #[inline]
    pub fn on_thread_stop(&self) {
        self.thread_alive_gauge.dec();
    }

    #[inline]
    pub fn on_thread_park(&self) {
        self.thread_idle_gauge.inc();
    }

    #[inline]
    pub fn on_thread_unpark(&self) {
        self.thread_idle_gauge.dec();
    }
}
