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

mod await_tree;
mod http_service;
mod jeprof;
mod metrics;
mod pprof;

use crate::http::await_tree::AwaitTreeHandler;
use crate::http::http_service::PoemHTTPServer;
use crate::http::jeprof::JeProfHandler;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::pprof::PProfHandler;
use crate::runtime::manager::RuntimeManager;
use once_cell::sync::Lazy;
use poem::RouteMethod;
pub static HTTP_SERVICE: Lazy<Box<PoemHTTPServer>> = Lazy::new(|| new_server());

/// Implement the own handlers for concrete components
pub trait Handler {
    fn get_route_method(&self) -> RouteMethod;
    fn get_route_path(&self) -> String;
}

pub trait HTTPServer: Send + Sync {
    fn start(&self, runtime_manager: RuntimeManager, port: u16);
    fn register_handler(&self, handler: impl Handler + 'static);
}

fn new_server() -> Box<PoemHTTPServer> {
    let server = PoemHTTPServer::new();
    server.register_handler(PProfHandler::default());
    server.register_handler(MetricsHTTPHandler::default());
    server.register_handler(AwaitTreeHandler::default());
    server.register_handler(JeProfHandler::default());
    Box::new(server)
}
