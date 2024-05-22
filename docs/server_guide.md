---
layout: page
displayTitle: Uniffle Shuffle Server Guide
title: Uniffle Shuffle Server Guide
description: Uniffle Shuffle Server Guide
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
# Uniffle Shuffle Server Guide

## Deploy
This document will introduce how to deploy Uniffle shuffle servers.

### Steps
1. unzip package to RSS_HOME
2. update RSS_HOME/bin/rss-env.sh, eg,
   ```
     JAVA_HOME=<java_home>
     HADOOP_HOME=<hadoop home>
     XMX_SIZE="80g"
   ```
   
   For the following cases, you don't need to specify `HADOOP_HOME` that will simplify the server deployment.
   1. using the storage type without HDFS like `MEMORY_LOCALFILE
   2. using HDFS and package with hadoop jars, like this: `./build_distribution.sh --hadoop-profile 'hadoop3.2' -Phadoop-dependencies-included`. But you need to explicitly set the `spark.rss.client.remote.storage.useLocalConfAsDefault=true`

3. update RSS_HOME/conf/server.conf, eg,
   ```
     rss.rpc.server.port 19999
     rss.jetty.http.port 19998
     rss.rpc.executor.size 2000
     # it should be configed the same as in coordinator
     rss.storage.type MEMORY_LOCALFILE_HDFS
     rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
     # local storage path for shuffle server
     rss.storage.basePath /data1/rssdata,/data2/rssdata....
     # it's better to config thread num according to local disk num
     rss.server.flush.thread.alive 5
     rss.server.flush.localfile.threadPool.size 10
     rss.server.flush.hadoop.threadPool.size 60
     rss.server.buffer.capacity 40g
     rss.server.read.buffer.capacity 20g
     rss.server.heartbeat.interval 10000
     rss.rpc.message.max.size 1073741824
     rss.server.preAllocation.expired 120000
     rss.server.commit.timeout 600000
     rss.server.app.expired.withoutHeartbeat 120000
     # note: the default value of rss.server.flush.cold.storage.threshold.size is 64m
     # there will be no data written to DFS if set it as 100g even rss.storage.type=MEMORY_LOCALFILE_HDFS
     # please set proper value if DFS is used, eg, 64m, 128m.
     rss.server.flush.cold.storage.threshold.size 100g
   ```
4. start Shuffle Server
   ```
    bash RSS_HOME/bin/start-shuffle-server.sh
   ```

## Configuration
| Property Name                                            | Default                                                                | Description                                                                                                                                                                                                                                                                                                                                                                                  |
|----------------------------------------------------------|------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rss.coordinator.quorum                                   | -                                                                      | Coordinator quorum                                                                                                                                                                                                                                                                                                                                                                           |
| rss.rpc.server.type                                      | GRPC                                                                   | Shuffle server type, supports GRPC_NETTY, GRPC. The default value is GRPC. But we recommend using GRPC_NETTY to enable Netty on the server side for better stability and performance.                                                                                                                                                                                                        |
| rss.rpc.server.port                                      | 19999                                                                  | RPC port for Shuffle server, if set zero, grpc server start on random port.                                                                                                                                                                                                                                                                                                                  |
| rss.jetty.http.port                                      | 19998                                                                  | Http port for Shuffle server                                                                                                                                                                                                                                                                                                                                                                 |
| rss.server.netty.port                                    | -1                                                                     | Netty port for Shuffle server, if set zero, Netty server start on random port.                                                                                                                                                                                                                                                                                                               |
| rss.server.netty.epoll.enable                            | false                                                                  | Whether to enable epoll model with Netty server.                                                                                                                                                                                                                                                                                                                                             |
| rss.server.netty.accept.thread                           | 10                                                                     | Accept thread count in netty.                                                                                                                                                                                                                                                                                                                                                                |
| rss.server.netty.worker.thread                           | 0                                                                      | Worker thread count in netty. When set to 0, the default value is dynamically set to twice the number of processor cores, but it will not be less than 100 to ensure the minimum throughput of the service.                                                                                                                                                                                  |
| rss.server.netty.connect.backlog                         | 0                                                                      | For Netty server, requested maximum length of the queue of incoming connections.                                                                                                                                                                                                                                                                                                             |
| rss.server.netty.connect.timeout                         | 5000                                                                   | Timeout for connection in netty.                                                                                                                                                                                                                                                                                                                                                             |
| rss.server.netty.receive.buf                             | 0                                                                      | Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps, buffer size should be ~ 1.25MB. Default is 0, the operating system automatically estimates the receive buffer size based on default settings.                                                          |
| rss.server.netty.send.buf                                | 0                                                                      | Send buffer size (SO_SNDBUF).                                                                                                                                                                                                                                                                                                                                                                |
| rss.server.buffer.capacity                               | -1                                                                     | Max memory of buffer manager for shuffle server. If negative, JVM heap size * buffer.ratio is used                                                                                                                                                                                                                                                                                           |
| rss.server.buffer.capacity.ratio                         | 0.6                                                                    | when `rss.server.buffer.capacity`=-1, then the buffer capacity is JVM heap size or off-heap size(when enabling Netty) * ratio                                                                                                                                                                                                                                                                |
| rss.server.memory.shuffle.highWaterMark.percentage       | 75.0                                                                   | Threshold of spill data to storage, percentage of rss.server.buffer.capacity                                                                                                                                                                                                                                                                                                                 |
| rss.server.memory.shuffle.lowWaterMark.percentage        | 25.0                                                                   | Threshold of keep data in memory, percentage of rss.server.buffer.capacity                                                                                                                                                                                                                                                                                                                   |
| rss.server.read.buffer.capacity                          | -1                                                                     | Max size of buffer for reading data. If negative, JVM heap size * read.buffer.ratio is used                                                                                                                                                                                                                                                                                                  |
| rss.server.read.buffer.capacity.ratio                    | 0.2                                                                    | when `rss.server.read.buffer.capacity`=-1, then read buffer capacity is JVM heap size or off-heap size(when enabling Netty) * ratio                                                                                                                                                                                                                                                          |
| rss.server.heartbeat.interval                            | 10000                                                                  | Heartbeat interval to Coordinator (ms)                                                                                                                                                                                                                                                                                                                                                       |
| rss.server.netty.metrics.pendingTaskNumPollingIntervalMs | 10000                                                                  | How often to collect Netty pending tasks number metrics (in milliseconds)                                                                                                                                                                                                                                                                                                                    |
| rss.server.flush.localfile.threadPool.size               | 10                                                                     | Thread pool for flush data to local file                                                                                                                                                                                                                                                                                                                                                     |
| rss.server.flush.hadoop.threadPool.size                  | 60                                                                     | Thread pool for flush data to hadoop storage                                                                                                                                                                                                                                                                                                                                                 |
| rss.server.commit.timeout                                | 600000                                                                 | Timeout when commit shuffle data (ms)                                                                                                                                                                                                                                                                                                                                                        |
| rss.storage.type                                         | -                                                                      | Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS                                                                                                                                                                                                                                                                                                                                |
| rss.server.flush.cold.storage.threshold.size             | 64M                                                                    | The threshold of data size for LOACALFILE and HADOOP if MEMORY_LOCALFILE_HDFS is used                                                                                                                                                                                                                                                                                                        |
| rss.server.tags                                          | -                                                                      | The comma-separated list of tags to indicate the shuffle server's attributes. It will be used as the assignment basis for the coordinator                                                                                                                                                                                                                                                    |
| rss.server.single.buffer.flush.enabled                   | true                                                                   | Whether single buffer flush when size exceeded rss.server.single.buffer.flush.threshold                                                                                                                                                                                                                                                                                                      |
| rss.server.single.buffer.flush.threshold                 | 128M                                                                   | The threshold of single shuffle buffer flush                                                                                                                                                                                                                                                                                                                                                 |
| rss.server.disk.capacity                                 | -1                                                                     | Disk capacity that shuffle server can use. If negative, it will use disk whole space * ratio                                                                                                                                                                                                                                                                                                 |
| rss.server.disk.capacity.ratio                           | 0.9                                                                    | When `rss.server.disk.capacity` is negative, disk whole space * ratio is used                                                                                                                                                                                                                                                                                                                |
| rss.server.hybrid.storage.fallback.strategy.class        | -                                                                      | The fallback strategy for `MEMORY_LOCALFILE_HDFS`. Support `org.apache.uniffle.server.storage.RotateStorageManagerFallbackStrategy`,`org.apache.uniffle.server.storage.LocalStorageManagerFallbackStrategy` and `org.apache.uniffle.server.storage.HadoopStorageManagerFallbackStrategy`. If not set, `org.apache.uniffle.server.storage.HadoopStorageManagerFallbackStrategy` will be used. |
| rss.server.leak.shuffledata.check.interval               | 3600000                                                                | The interval of leak shuffle data check (ms)                                                                                                                                                                                                                                                                                                                                                 |
| rss.server.max.concurrency.of.per-partition.write        | 30                                                                     | The max concurrency of single partition writer, the data partition file number is equal to this value. Default value is 1. This config could improve the writing speed, especially for huge partition.                                                                                                                                                                                       |
| rss.server.max.concurrency.limit.of.per-partition.write  | -                                                                      | The limit for max concurrency per-partition write specified by client, this won't be enabled by default.                                                                                                                                                                                                                                                                                     |
| rss.metrics.reporter.class                               | -                                                                      | The class of metrics reporter.                                                                                                                                                                                                                                                                                                                                                               |
| rss.server.hybrid.storage.manager.selector.class         | org.apache.uniffle.server.storage.hybrid.DefaultStorageManagerSelector | The manager selector strategy for `MEMORY_LOCALFILE_HDFS`. Default value is `DefaultStorageManagerSelector`, and another `HugePartitionSensitiveStorageManagerSelector` will flush only huge partition's data to cold storage.                                                                                                                                                               |
| rss.server.disk-capacity.watermark.check.enabled         | false                                                                  | If it is co-located with other services, the high-low watermark check based on the uniffle used is not correct. Due to this, the whole disk capacity watermark check is necessary, which will reuse the current watermark value. It will be disabled by default.                                                                                                                             |

### Advanced Configurations
| Property Name                                    | Default | Description                                                                                                                                                                                 |
|--------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rss.server.storageMediaProvider.from.env.key     | -       | Sometimes, the local storage type/media info is provided by external system. RSS would read the env key defined by this configuration and get info about the storage media of its basePaths |
| rss.server.decommission.check.interval           | 60000   | The interval(ms) to check if all applications have finish when server is decommissioning                                                                                                    |
| rss.server.decommission.shutdown                 | true    | Whether shutdown the server after server is decommissioned                                                                                                                                  |
| rss.server.health.checker.script.path            | -       | The health script path for `HealthScriptChecker`. To enable `HealthScriptChecker`, need to set `rss.server.health.checker.class.names` and set `rss.server.health.check.enable` to true.    |
| rss.server.health.checker.script.execute.timeout | 5000    | Timeout for `HealthScriptChecker` execute health script.(ms)                                                                                                                                |

### Huge Partition Optimization
A huge partition is a common problem for Spark/MR and so on, caused by data skew. And it can cause the shuffle server to become unstable. To solve this, we introduce some mechanisms to limit the writing of huge partitions to avoid affecting regular partitions, more details can be found in [ISSUE-378](https://github.com/apache/incubator-uniffle/issues/378). The basic rules for limiting large partitions are memory usage limits and flushing individual buffers directly to persistent storage.

#### Memory usage limit
To do this, we introduce the extra configs

| Property Name                                | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|----------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rss.server.huge-partition.size.threshold     | 20g     | Threshold of huge partition size, once exceeding threshold, memory usage limitation and huge partition buffer flushing will be triggered. This value depends on the capacity of per disk in shuffle server. For example, per disk capacity is 1TB, and the max size of huge partition in per disk is 5. So the total size of huge partition in local disk is 100g (10%)，this is an acceptable config value. Once reaching this threshold, it will be better to flush data to HADOOP FS directly, which could be handled by multiple storage manager fallback strategy |
| rss.server.huge-partition.memory.limit.ratio | 0.2     | The memory usage limit ratio for huge partition, it will only triggered when partition's size exceeds the threshold of 'rss.server.huge-partition.size.threshold'. If the buffer capacity is 10g, this means the default memory usage for huge partition is 2g. Similarly, this config value depends on max size of huge partitions on per shuffle server.                                                                                                                                                                                                            |

#### Data flush
Once the huge partition threshold is reached, the partition is marked as a huge partition. And then single buffer flush is triggered (writing to persistent storage as soon as possible). By default, single buffer flush is only enabled by configuring `rss.server.single.buffer.flush.enabled`, but it's automatically valid for huge partition. 

If you don't use HADOOP FS, the huge partition may be flushed to local disk, which is dangerous if the partition size is larger than the free disk space. Therefore, it is recommended to use a mixed storage type, including HDFS or other distributed file systems.

For HADOOP FS, the conf value of `rss.server.single.buffer.flush.threshold` should be greater than the value of `rss.server.flush.cold.storage.threshold.size`, which will flush data directly to Hadoop FS. 

Finally, to improve the speed of writing to HDFS for a single partition, the value of `rss.server.max.concurrency.of.per-partition.write` and `rss.server.flush.hdfs.threadPool.size` could be increased to 50 or 100.

### Netty
In version 0.8.0, we introduced Netty. Enabling Netty on ShuffleServer can significantly reduce GC time in high-throughput scenarios. We can enable Netty through the parameters `rss.server.netty.port` and `rss.rpc.server.type`. Note: After setting the parameter `rss.rpc.server.type` to `GRPC_NETTY`, ShuffleServer will be tagged with `GRPC_NETTY`, that is, the node can only be assigned to clients with `spark.rss.client.type=GRPC_NETTY`.

When enabling Netty, we should also consider memory related configurations, the following is an example.

#### rss-env.sh
```
XMX_SIZE=20g
MAX_DIRECT_MEMORY_SIZE=120g
```
#### server.conf
```
rss.server.buffer.capacity 110g
rss.server.read.buffer.capacity 5g
```

#### Example of server conf
```
rss.rpc.server.port 19999
rss.jetty.http.port 19998
rss.rpc.server.type GRPC_NETTY
rss.server.netty.port 17000
rss.rpc.executor.size 2000
rss.storage.type MEMORY_LOCALFILE_HDFS
rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
rss.storage.basePath /data1/rssdata,/data2/rssdata....
rss.server.flush.thread.alive 10
rss.server.buffer.capacity 110g
rss.server.read.buffer.capacity 5g
rss.server.heartbeat.interval 10000
rss.rpc.message.max.size 1073741824
rss.server.preAllocation.expired 120000
rss.server.commit.timeout 600000
rss.server.app.expired.withoutHeartbeat 120000

# For huge partitions
rss.server.flush.localfile.threadPool.size 20
rss.server.flush.hadoop.threadPool.size 60
rss.server.flush.cold.storage.threshold.size 128m
rss.server.single.buffer.flush.threshold 129m
rss.server.max.concurrency.of.per-partition.write 30
rss.server.huge-partition.size.threshold 20g
rss.server.huge-partition.memory.limit.ratio 0.2
```
