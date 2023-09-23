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

#![feature(impl_trait_in_assoc_type)]

use crate::app::{AppManager, AppManagerRef};
use crate::config::{Config, LogConfig, RotationConfig};
use crate::grpc::grpc_middleware::AwaitTreeMiddlewareLayer;
use crate::grpc::{DefaultShuffleServer, MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use crate::http::{HTTPServer, HTTP_SERVICE};
use crate::metric::configure_metric_service;
use crate::proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::proto::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId};
use crate::util::{gen_worker_uid, get_local_ip};
use anyhow::Result;
use log::info;

use crate::await_tree::AWAIT_TREE_REGISTRY;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tonic::transport::{Channel, Server};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

pub mod app;
mod await_tree;
mod config;
mod error;
pub mod grpc;
mod http;
mod mem_allocator;
mod metric;
pub mod proto;
mod readable_size;
pub mod store;
mod util;

const DEFAULT_SHUFFLE_SERVER_TAG: &str = "ss_v4";

async fn schedule_coordinator_report(
    app_manager: AppManagerRef,
    coordinator_quorum: Vec<String>,
    grpc_port: i32,
    tags: Vec<String>,
    worker_uid: String,
) -> anyhow::Result<()> {
    tokio::spawn(async move {
        let ip = get_local_ip().unwrap().to_string();

        info!("machine ip: {}", ip.clone());

        let shuffle_server_id = ShuffleServerId {
            id: worker_uid,
            ip,
            port: grpc_port,
            netty_port: 0,
        };

        let mut multi_coordinator_clients: Vec<CoordinatorServerClient<Channel>> =
            futures::future::try_join_all(
                coordinator_quorum
                    .iter()
                    .map(|quorum| CoordinatorServerClient::connect(format!("http://{}", quorum))),
            )
            .await
            .unwrap();

        loop {
            // todo: add interval as config var
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut all_tags = vec![];
            all_tags.push(DEFAULT_SHUFFLE_SERVER_TAG.to_string());
            all_tags.extend_from_slice(&*tags);

            let healthy = app_manager.store_is_healthy().await.unwrap_or(false);
            let memory_snapshot = app_manager
                .store_memory_snapshot()
                .await
                .unwrap_or((0, 0, 0).into());
            let memory_spill_event_num =
                app_manager.store_memory_spill_event_num().unwrap_or(0) as i32;

            let heartbeat_req = ShuffleServerHeartBeatRequest {
                server_id: Some(shuffle_server_id.clone()),
                used_memory: memory_snapshot.get_used(),
                pre_allocated_memory: memory_snapshot.get_allocated(),
                available_memory: memory_snapshot.get_capacity()
                    - memory_snapshot.get_used()
                    - memory_snapshot.get_allocated(),
                event_num_in_flush: memory_spill_event_num,
                tags: all_tags,
                is_healthy: Some(healthy),
                status: 0,
                storage_info: Default::default(),
            };

            // It must use the 0..len to avoid borrow check in loop.
            for idx in 0..multi_coordinator_clients.len() {
                let client = multi_coordinator_clients.get_mut(idx).unwrap();
                let _ = client
                    .heartbeat(tonic::Request::new(heartbeat_req.clone()))
                    .await;
            }
        }
    });

    Ok(())
}

const LOG_FILE_NAME: &str = "uniffle-worker.log";

fn init_log(log: &LogConfig) -> WorkerGuard {
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, LOG_FILE_NAME),
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, LOG_FILE_NAME),
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, LOG_FILE_NAME),
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_line_number(true)
        .with_writer(non_blocking);

    let console_layer = console_subscriber::Builder::default()
        .server_addr((Ipv4Addr::new(0, 0, 0, 0), 21002))
        .spawn();

    Registry::default()
        .with(env_filter)
        .with(formatting_layer)
        .with(file_layer)
        .with(console_layer)
        .init();

    // Note: _guard is a WorkerGuard which is returned by tracing_appender::non_blocking to
    // ensure buffered logs are flushed to their output in the case of abrupt terminations of a process.
    // See WorkerGuard module for more details.
    _guard
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::create_from_env();

    // init log
    let log_config = &config.log.clone().unwrap_or(Default::default());
    let _guard = init_log(log_config);

    let rpc_port = config.grpc_port.unwrap_or(19999);
    let worker_uid = gen_worker_uid(rpc_port);

    let metric_config = config.metrics.clone();
    configure_metric_service(&metric_config, worker_uid.clone());

    let coordinator_quorum = config.coordinator_quorum.clone();
    let tags = config.tags.clone().unwrap_or(vec![]);
    let app_manager_ref = AppManager::get_ref(config.clone());
    let _ = schedule_coordinator_report(
        app_manager_ref.clone(),
        coordinator_quorum,
        rpc_port,
        tags,
        worker_uid,
    )
    .await;

    let http_port = config.http_monitor_service_port.unwrap_or(20010);
    info!(
        "Starting http monitor service with port:[{}] ......",
        http_port
    );
    HTTP_SERVICE.start(http_port);

    info!("Starting GRpc server with port:[{}] ......", rpc_port);
    let shuffle_server = DefaultShuffleServer::from(app_manager_ref);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
    let service = ShuffleServerServer::new(shuffle_server)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .tcp_nodelay(true)
        .layer(AwaitTreeMiddlewareLayer::new_optional(Some(
            AWAIT_TREE_REGISTRY.clone(),
        )))
        .add_service(service)
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::get_local_ip;

    #[test]
    fn get_local_ip_test() {
        let ip = get_local_ip().unwrap();
        println!("{}", ip.to_string());
    }
}
