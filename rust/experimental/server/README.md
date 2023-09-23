Another implementation of Apache Uniffle shuffle server

## Benchmark report

#### Environment
_Software_: Uniffle 0.7.0 / Hadoop 3.2.2 / Spark 3.1.2

_Hardware_: Machine 96 cores, 512G memory, 1T * 4 SSD, network bandwidth 8GB/s

_Hadoop Yarn Cluster_: 1 * ResourceManager + 40 * NodeManager, every machine 4T * 4 HDD

_Uniffle Cluster_: 1 * Coordinator + 5 * Shuffle Server, every machine 1T * 4 SSD

#### Configuration
spark's conf
``` 
spark.executor.instances 100
spark.executor.cores 1
spark.executor.memory 2g
spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
spark.rss.storage.type MEMORY_LOCALFILE
``` 

uniffle grpc-based server's conf
``` 
JVM XMX=130g

...
rss.server.buffer.capacity 100g
rss.server.read.buffer.capacity 20g
rss.server.flush.thread.alive 10
rss.server.flush.threadPool.size 50
rss.server.high.watermark.write 80
rss.server.low.watermark.write 70
...
``` 

uniffle-x(Rust-based)'s conf
```
store_type = "MEMORY_LOCALFILE"

[memory_store]
capacity = "100G"

[localfile_store]
data_paths = ["/data1/uniffle", "/data2/uniffle", "/data3/uniffle", "/data4/uniffle"]
healthy_check_min_disks = 0

[hybrid_store]
memory_spill_high_watermark = 0.5
memory_spill_low_watermark = 0.4
``` 

#### Tera Sort
| type                         |       100G       |           1T          | 5T (run with 400 executors) |
|------------------------------|:----------------:|:---------------------:|:---------------------------:|
| vanilla uniffle (grpc-based) | 1.4min (29s/53s) | 12min (4.7min/7.0min) |    18.7min(12min/6.7min)    |
| uniffle-x                    | 1.3min (28s/48s) | 11min (4.3min/6.5min) |    14min(7.8min/6.2min)     |

> Tips: When running 5T on vanilla Uniffle, data sent timeouts may occur, and there can be occasional failures in client fetching the block bitmap.

## Build

`cargo build --release`

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
data_path = "hdfs://rbf-x/user/bi"
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

```shell
export JAVA_HOME=/path/to/java
export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}

export HADOOP_HOME=/path/to/hadoop
export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)

cargo build --features hdfs --release
``` 

## Profiling

### Tokio console
1. build with unstable tokio for uniffle-worker binary (this has been enabled by default)
    ```shell
    cargo build
    ```
2. worker run with tokio-console. the log level of `trace` must be enabled
    ```shell
    WORKER_IP={ip} RUST_LOG=trace WORKER_CONFIG_PATH=./config.toml ./uniffle-worker
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