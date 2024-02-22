<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

Another implementation of Apache Uniffle shuffle server (Single binary, no extra dependencies)

## Benchmark report

#### Environment

_Software_: Uniffle 0.8.0 / Hadoop 3.2.2 / Spark 3.1.2

_Hardware_: Machine 96 cores, 512G memory, 1T * 4 SSD, network bandwidth 8GB/s

_Hadoop Yarn Cluster_: 1 * ResourceManager + 40 * NodeManager, every machine 4T * 4 HDD

_Uniffle Cluster_: 1 * Coordinator + 1 * Shuffle Server, every machine 1T * 4 NVME SSD

#### Configuration

spark's conf
```yaml
spark.executor.instances 400
spark.executor.cores 1
spark.executor.memory 2g
spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
spark.rss.storage.type MEMORY_LOCALFILE
``` 

uniffle grpc-based server's conf
``` yaml
JVM XMX=30g

# JDK11 + G1 

rss.server.buffer.capacity 10g
rss.server.read.buffer.capacity 10g
rss.server.flush.thread.alive 10
rss.server.flush.threadPool.size 50
rss.server.high.watermark.write 80
rss.server.low.watermark.write 70
``` 

Rust-based shuffle-server conf
```
store_type = "MEMORY_LOCALFILE"

[memory_store]
capacity = "10G"

[localfile_store]
data_paths = ["/data1/uniffle", "/data2/uniffle", "/data3/uniffle", "/data4/uniffle"]
healthy_check_min_disks = 0

[hybrid_store]
memory_spill_high_watermark = 0.8
memory_spill_low_watermark = 0.7
``` 

#### TeraSort cost times
| type/buffer capacity                 | 250G (compressed)  |                                 comment                                  |
|--------------------------------------|:------------------:|:------------------------------------------------------------------------:|
| vanilla spark ess                    |  5.0min (2.2/2.8)  | ess use 400 nodes but uniffle only one. But the rss speed is still fast! | 
| vanilla uniffle (grpc-based)  / 10g  |  5.3min (2.3m/3m)  |                                  1.9G/s                                  |
| vanilla uniffle (grpc-based)  / 300g | 5.6min (3.7m/1.9m) |                      GC occurs frequently / 2.5G/s                       |
| vanilla uniffle (netty-based) / 10g  |         /          |          read failed. 2.5G/s (write is better due to zero copy)          |
| vanilla uniffle (netty-based) / 300g |         /          |                                 app hang                                 |
| rust based shuffle server     / 10g  | 4.6min (2.2m/2.4m) |                                 2.4 G/s                                  |
| rust based shuffle server     / 300g |  4min (1.5m/2.5m)  |                                 3.5 G/s                                  |


Compared with grpc based server, rust-based server has less memory footprint and stable performance.  

And Netty is still not stable for production env.

In the future, rust-based server will use io_uring mechanism to improve writing performance.

## Build

`cargo build --release --features hdfs,jemalloc`

Uniffle-x currently treats all compiler warnings as error, with some dead-code warning excluded. When you are developing
and really want to ignore the warnings for now, you can use `ccargo --config 'build.rustflags=["-W", "warnings"]' build`
to restore the default behavior. However, before submit your pr, you should fix all the warnings.


## Config

config.toml as follows:

``` 
store_type = "MEMORY_LOCALFILE"
grpc_port = 21100
coordinator_quorum = ["xxxxxxx1", "xxxxxxx2]
tags = ["uniffle-worker"]

[memory_store]
capacity = "100G"

[localfile_store]
data_paths = ["/data1/uniffle", "/data2/uniffle"]
healthy_check_min_disks = 0

[hdfs_store]
max_concurrency = 10

[hybrid_store]
memory_spill_high_watermark = 0.8
memory_spill_low_watermark = 0.2
memory_single_buffer_max_spill_size = "256M"

[metrics]
http_port = 19998
push_gateway_endpoint = "http://xxxxxxxxxxxxxx/pushgateway"
``` 

## Run

`WORKER_IP={ip} RUST_LOG=info WORKER_CONFIG_PATH=./config.toml ./uniffle-worker`

### HDFS Setup 

Benefit from the hdfs-native crate, there is no need to setup the JAVA_HOME and relative dependencies.
If HDFS store is valid, the spark client must specify the conf of `spark.rss.client.remote.storage.useLocalConfAsDefault=true`

```shell
cargo build --features hdfs --release
```

```shell
# configure the kerberos and conf env
HADOOP_CONF_DIR=/etc/hadoop/conf KRB5_CONFIG=/etc/krb5.conf KRB5CCNAME=/tmp/krb5cc_2002 LOG=info ./uniffle-worker
```

## Profiling

### Tokio console
1. build with unstable tokio for uniffle-worker binary (this has been enabled by default)
    ```shell
    cargo build
    ```
2. worker run with tokio-console. the log level of `trace` must be enabled
    ```shell
    WORKER_IP={ip} RUST_LOG=trace ./uniffle-worker -c ./config.toml 
    ```
3. tokio-console client side connect
    ```shell
   tokio-console http://{uniffle-worker-host}:21002
    ```
   
### Heap profiling
1. build with profile support
    ```shell
    cargo build --release --features memory-prof
    ```
2. Start with profile
    ```shell
    _RJEM_MALLOC_CONF=prof:true,prof_prefix:jeprof.out ./uniffle-worker
    ```
   
### CPU Profiling
1. build with jemalloc feature
    ```shell
    cargo build --release --features jemalloc
    ```
2. Paste following command to get cpu profile flamegraph
    ```shell
    go tool pprof -http="0.0.0.0:8081" http://{remote_ip}:8080/debug/pprof/profile?seconds=30
    ```
   - localhost:8080: riffle server.
   - remote_ip: pprof server address.
   - seconds=30: Profiling lasts for 30 seconds.
   
   Then open the URL <your-ip>:8081/ui/flamegraph in your browser to view the flamegraph:
   