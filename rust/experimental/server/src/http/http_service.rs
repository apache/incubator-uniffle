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

use crate::error::WorkerError;

use poem::endpoint::make_sync;
use poem::error::ResponseError;
use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::{get, Route, RouteMethod, Server};

use std::sync::Mutex;

use crate::http::{HTTPServer, Handler};
use crate::runtime::manager::RuntimeManager;
impl ResponseError for WorkerError {
    fn status(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

struct IndexPageHandler {}
impl Handler for IndexPageHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make_sync(|_| "Hello uniffle server"))
    }

    fn get_route_path(&self) -> String {
        "/".to_string()
    }
}

pub struct PoemHTTPServer {
    handlers: Mutex<Vec<Box<dyn Handler>>>,
}

unsafe impl Send for PoemHTTPServer {}
unsafe impl Sync for PoemHTTPServer {}

impl PoemHTTPServer {
    pub fn new() -> Self {
        let handlers: Vec<Box<dyn Handler>> = vec![Box::new(IndexPageHandler {})];
        Self {
            handlers: Mutex::new(handlers),
        }
    }
}

impl HTTPServer for PoemHTTPServer {
    fn start(&self, runtime_manager: RuntimeManager, port: u16) {
        let mut app = Route::new();
        let handlers = self.handlers.lock().unwrap();
        for handler in handlers.iter() {
            app = app.at(handler.get_route_path(), handler.get_route_method());
        }
        runtime_manager.http_runtime.spawn(async move {
            let _ = Server::new(TcpListener::bind(format!("0.0.0.0:{}", port)))
                .name("uniffle-server-http-service")
                .run(app)
                .await;
        });
    }

    fn register_handler(&self, handler: impl Handler + 'static) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(Box::new(handler));
    }
}
