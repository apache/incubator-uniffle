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

use crate::http::Handler;
use crate::metric::REGISTRY;
use log::error;
use poem::endpoint::make_sync;
use poem::{get, RouteMethod};

pub struct MetricsHTTPHandler {}

impl Default for MetricsHTTPHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for MetricsHTTPHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make_sync(|_| {
            use prometheus::Encoder;
            let encoder = prometheus::TextEncoder::new();

            let mut buffer = Vec::new();
            if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
                error!("could not encode custom metrics: {:?}", e);
            };
            let mut res = match String::from_utf8(buffer.clone()) {
                Ok(v) => v,
                Err(e) => {
                    error!("custom metrics could not be from_utf8'd: {:?}", e);
                    String::default()
                }
            };
            buffer.clear();

            let mut buffer = Vec::new();
            if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
                error!("could not encode prometheus metrics: {:?}", e);
            };
            let res_custom = match String::from_utf8(buffer.clone()) {
                Ok(v) => v,
                Err(e) => {
                    error!("prometheus metrics could not be from_utf8'd: {}", e);
                    String::default()
                }
            };
            buffer.clear();

            res.push_str(&res_custom);
            res
        }))
    }

    fn get_route_path(&self) -> String {
        "/metrics".to_string()
    }
}
