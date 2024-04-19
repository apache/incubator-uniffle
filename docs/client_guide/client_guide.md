---
layout: page
displayTitle: Uniffle Shuffle Client Guide
title: Uniffle Shuffle Client Guide
description: Uniffle Shuffle Client Guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
# Uniffle Shuffle Client Guide

Uniffle is designed as a unified shuffle engine for multiple computing frameworks, including Apache Spark, Apache Hadoop and Apache Tez.
Uniffle has provided pluggable client plugins to enable remote shuffle in Spark, MapReduce and Tez.

## Deploy & client specific configuration
Refer to the following documents on how to deploy Uniffle client plugins with Spark, MapReduce and Tez. Client specific configurations are also listed in each documents.
|Client|Link|
|---|---|
|Spark|[Deploy Spark Client Plugin & Configurations](spark_client_guide.md)|
|MapReduce|[Deploy MapReduce Client Plugin & Configurations](mr_client_guide.md)|
|Tez|[Deploy Tez Client Plugin & Configurations](tez_client_guide.md)|


## Common Configuration

The important configuration of client is listed as following. These configurations are shared by all types of clients.

|Property Name|Default| Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|---|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|<client_type>.rss.coordinator.quorum|-| Coordinator quorum                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|<client_type>.rss.writer.buffer.size|3m| Buffer size for single partition data                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|<client_type>.rss.storage.type|-| Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS                                                                                                                                                                                                                                                                                                                                                                                                                                |
|<client_type>.rss.client.read.buffer.size|14m| The max data size read from storage                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|<client_type>.rss.client.send.threadPool.size|5| The thread size for send shuffle data to shuffle server                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|<client_type>.rss.client.assignment.tags|-| The comma-separated list of tags for deciding assignment shuffle servers. Notice that the SHUFFLE_SERVER_VERSION will always as the assignment tag whether this conf is set or not                                                                                                                                                                                                                                                                                                           |
|<client_type>.rss.client.data.commit.pool.size|The number of assigned shuffle servers| The thread size for sending commit to shuffle servers                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|<client_type>.rss.client.assignment.shuffle.nodes.max|-1| The number of required assignment shuffle servers. If it is less than 0 or equals to 0 or greater than the coordinator's config of "rss.coordinator.shuffle.nodes.max", it will use the size of "rss.coordinator.shuffle.nodes.max" default                                                                                                                                                                                                                                                  |
|<client_type>.rss.client.io.compression.codec|lz4| The compression codec is used to compress the shuffle data. Default codec is `lz4`. Other options are`ZSTD` and `SNAPPY`.                                                                                                                                                                                                                                                                                                                                                                    |
|<client_type>.rss.client.io.compression.zstd.level|3| The zstd compression level, the default level is 3                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|<client_type>.rss.client.shuffle.data.distribution.type|NORMAL| The type of partition shuffle data distribution, including normal and local_order. The default value is normal. Now this config is only valid in Spark3.x                                                                                                                                                                                                                                                                                                                                    |
|<client_type>.rss.estimate.task.concurrency.dynamic.factor|1.0| Between 0 and 1, used to estimate task concurrency, when the client is spark, it represents how likely is this part of the resource between spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors to be allocated, when the client is mr, it represents how likely the resources of map and reduce are satisfied. Effective when <client_type>.rss.estimate.server.assignment.enabled=true or Coordinator's rss.coordinator.select.partition.strategy is CONTINUOUS. |
|<client_type>.rss.estimate.server.assignment.enabled|false| Support mr and spark, whether to enable estimation of the number of ShuffleServers that need to be allocated based on the number of concurrent tasks.                                                                                                                                                                                                                                                                                                                                        |
|<client_type>.rss.estimate.task.concurrency.per.server|80| It takes effect when rss.estimate.server.assignment.enabled=true, how many tasks are concurrently assigned to a ShuffleServer.                                                                                                                                                                                                                                                                                                                                                               |
|<client_type>.rss.client.max.concurrency.of.per-partition.write|-| The maximum number of files that can be written concurrently to a single partition is determined. This value will only be respected by the remote shuffle server if it is greater than 0.                                                                                                                                                                                                                                                                                                    |
|<client_type>.rss.client.rpc.timeout.ms|60000| Timeout in milliseconds for RPC calls.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|<client_type>.rss.client.rpc.maxAttempts|3| When we fail to send RPC calls, we will retry for maxAttempts times.                                                                                                                                                                                                                                                                                                                                                                                                                         |
Notice:

1. `<client_type>` should be `mapreduce` `tez` or `spark`

2. `<client_type>.rss.coordinator.quorum` is compulsory, and other configurations are optional when coordinator dynamic configuration is enabled.


### Client Quorum Setting 

Uniffle supports client-side quorum protocol to tolerant shuffle server crash. 
This feature is client-side behaviour, in which shuffle writer sends each block to multiple servers, and shuffle readers could fetch block data from one of server.
Since sending multiple replicas of blocks can reduce the shuffle performance and resource consumption, we designed it as an optional feature.

|Property Name|Default|Description|
|---|---|---|
|<client_type>.rss.data.replica|1|The max server number that each block can be send by client in quorum protocol|
|<client_type>.rss.data.replica.write|1|The min server number that each block should be send by client successfully|
|<client_type>.rss.data.replica.read|1|The min server number that metadata should be fetched by client successfully |

Notice: 

1. `spark.rss.data.replica.write` + `spark.rss.data.replica.read` > `spark.rss.data.replica`

Recommended examples:

1. Performance First (default)
```
spark.rss.data.replica 1
spark.rss.data.replica.write 1
spark.rss.data.replica.read 1
```

2. Fault-tolerant First
```
spark.rss.data.replica 3
spark.rss.data.replica.write 2
spark.rss.data.replica.read 2
```

### Netty Setting
| Property Name                                       | Default | Description                                                                                                                                                                                                                                                                                                                         |
|-----------------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <client_type>.rss.client.type                               | GRPC    | The default is GRPC, we can set it to GRPC_NETTY to enable the Netty on the client                                                                                                                                                                                                                                                  |
| <client_type>.rss.client.netty.io.mode                      | NIO     | Netty EventLoopGroup backend, available options: NIO, EPOLL.                                                                                                                                                                                                                                                                        |
| <client_type>.rss.client.netty.client.connection.timeout.ms | 600000  | Connection active timeout.                                                                                                                                                                                                                                                                                                          |
| <client_type>.rss.client.netty.client.threads               | 0       | Number of threads used in the client thread pool. Default is 0, Netty will use the number of (available logical cores * 2) as the number of threads.                                                                                                                                                                                |
| <client_type>.rss.client.netty.client.prefer.direct.bufs    | true    | If true, we will prefer allocating off-heap byte buffers within Netty.                                                                                                                                                                                                                                                              |
| <client_type>.rss.client.netty.client.connections.per.peer  | 2       | Suppose there are 100 executors, spark.rss.client.netty.client.connections.per.peer = 2, then each ShuffleServer will establish a total of (100 * 2) connections with multiple clients.                                                                                                                                             |
| <client_type>.rss.client.netty.client.receive.buffer        | 0       | Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps, buffer size should be ~ 1.25MB. Default is 0, the operating system automatically estimates the receive buffer size based on default settings. |
| <client_type>.rss.client.netty.client.send.buffer           | 0       | Send buffer size (SO_SNDBUF).                                                                                                                                                                                                                                                                                                       |
