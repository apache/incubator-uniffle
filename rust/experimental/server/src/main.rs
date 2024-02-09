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

use crate::app::{AppManager, AppManagerRef, SHUFFLE_SERVER_ID};
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::{Config, LogConfig, RotationConfig};
use crate::grpc::await_tree_middleware::AwaitTreeMiddlewareLayer;
use crate::grpc::metrics_middleware::MetricsMiddlewareLayer;
use crate::grpc::{DefaultShuffleServer, MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use crate::http::{HTTPServer, HTTP_SERVICE};
use crate::metric::{init_metric_service, GRPC_LATENCY_TIME_SEC};
use crate::proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::proto::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId};
use crate::runtime::manager::RuntimeManager;
use crate::signal::details::graceful_wait_for_signal;
use crate::util::{gen_worker_uid, get_local_ip};

use crate::mem_allocator::ALLOCATOR;
use crate::readable_size::ReadableSize;
use anyhow::Result;
use clap::{App, Arg};
use log::{debug, error, info};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
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
pub mod runtime;
pub mod signal;
pub mod store;
mod util;

const MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY: &str = "MAX_MEMORY_ALLOCATION_SIZE";
const DEFAULT_SHUFFLE_SERVER_TAG: &str = "ss_v4";

fn start_coordinator_report(
    runtime_manager: RuntimeManager,
    app_manager: AppManagerRef,
    coordinator_quorum: Vec<String>,
    grpc_port: i32,
    tags: Vec<String>,
    worker_uid: String,
) -> anyhow::Result<()> {
    runtime_manager.default_runtime.spawn(async move {
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

fn main() -> Result<()> {
    setup_max_memory_allocation();

    let args_match = App::new("Uniffle Worker")
        .version("0.9.0-SNAPSHOT")
        .about("Rust based shuffle server for Apache Uniffle")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .default_value("./config.toml")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .get_matches();

    let config_path = args_match.value_of("config").unwrap_or("./config.toml");
    let config = Config::from(config_path);

    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());

    // init log
    let log_config = &config.log.clone().unwrap_or(Default::default());
    let _guard = init_log(log_config);

    let rpc_port = config.grpc_port.unwrap_or(19999);
    let worker_uid = gen_worker_uid(rpc_port);
    // todo: remove some unnecessary worker_id transfer.
    SHUFFLE_SERVER_ID.get_or_init(|| worker_uid.clone());

    let metric_config = config.metrics.clone();
    init_metric_service(runtime_manager.clone(), &metric_config, worker_uid.clone());

    let coordinator_quorum = config.coordinator_quorum.clone();
    let tags = config.tags.clone().unwrap_or(vec![]);
    let app_manager_ref = AppManager::get_ref(runtime_manager.clone(), config.clone());
    let _ = start_coordinator_report(
        runtime_manager.clone(),
        app_manager_ref.clone(),
        coordinator_quorum,
        rpc_port,
        tags,
        worker_uid,
    );

    let http_port = config.http_monitor_service_port.unwrap_or(20010);
    info!(
        "Starting http monitor service with port:[{}] ......",
        http_port
    );
    HTTP_SERVICE.start(runtime_manager.clone(), http_port);

    let (tx, _) = broadcast::channel(1);

    info!("Starting GRpc server with port:[{}] ......", rpc_port);

    let available_cores = std::thread::available_parallelism()?;
    debug!("GRpc service with parallelism: [{}]", &available_cores);

    for _ in 0..available_cores.into() {
        let shuffle_server = DefaultShuffleServer::from(app_manager_ref.clone());
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
        let service = ShuffleServerServer::new(shuffle_server)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let service_tx = tx.subscribe();
        runtime_manager
            .grpc_runtime
            .spawn(async move { grpc_serve(service, addr, service_tx).await });
    }

    graceful_wait_for_signal(tx);

    Ok(())
}

fn setup_max_memory_allocation() {
    let _ = std::env::var(MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY).map(|v| {
        let readable_size = ReadableSize::from_str(v.as_str()).unwrap();
        ALLOCATOR.set_limit(readable_size.as_bytes() as usize)
    });
}

async fn grpc_serve(
    service: ShuffleServerServer<DefaultShuffleServer>,
    addr: SocketAddr,
    mut rx: broadcast::Receiver<()>,
) {
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    sock.set_reuse_address(true).unwrap();
    sock.set_reuse_port(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&addr.into()).unwrap();
    sock.listen(8192).unwrap();

    let incoming =
        tokio_stream::wrappers::TcpListenerStream::new(TcpListener::from_std(sock.into()).unwrap());

    Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .tcp_nodelay(true)
        .layer(MetricsMiddlewareLayer::new(GRPC_LATENCY_TIME_SEC.clone()))
        .layer(AwaitTreeMiddlewareLayer::new_optional(Some(
            AWAIT_TREE_REGISTRY.clone(),
        )))
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async {
            if let Err(err) = rx.recv().await {
                error!("Errors on stopping the GRPC service, err: {:?}.", err);
            } else {
                debug!("GRPC service has been graceful stopped.");
            }
        })
        .await
        .unwrap();
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
