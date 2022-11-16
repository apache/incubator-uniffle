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

Uniffle is designed as a unified shuffle engine for multiple computing frameworks, including Apache Spark and Apache Hadoop.
Uniffle has provided pluggable client plugins to enable remote shuffle in Spark and MapReduce.

## Deploy
This document will introduce how to deploy Uniffle client plugins with Spark and MapReduce.

### Deploy Spark Client Plugin

1. Add client jar to Spark classpath, eg, SPARK_HOME/jars/

   The jar for Spark2 is located in <RSS_HOME>/jars/client/spark2/rss-client-XXXXX-shaded.jar

   The jar for Spark3 is located in <RSS_HOME>/jars/client/spark3/rss-client-XXXXX-shaded.jar

2. Update Spark conf to enable Uniffle, eg,

   ```
   spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
   spark.rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
   # Note: For Spark2, spark.sql.adaptive.enabled should be false because Spark2 doesn't support AQE.
   ```

### Support Spark Dynamic Allocation

To support spark dynamic allocation with Uniffle, spark code should be updated.
There are 2 patches for spark-2.4.6 and spark-3.1.2 in spark-patches folder for reference.

After apply the patch and rebuild spark, add following configuration in spark conf to enable dynamic allocation:
  ```
  spark.shuffle.service.enabled false
  spark.dynamicAllocation.enabled true
  ```

### Support Spark AQE

To improve performance of AQE skew optimization, uniffle introduces the LOCAL_ORDER shuffle-data distribution mechanism 
and Continuous partition assignment mechanism.

1. LOCAL_ORDER shuffle-data distribution mechanism filter the lots of data to reduce network bandwidth and shuffle-server local-disk pressure.

    It can be enabled by the following config
      ```bash
      # Default value is NORMAL, it will directly append to file when the memory data is flushed to external storage 
      spark.rss.client.shuffle.data.distribution.type LOCAL_ORDER
      ```

2. Continuous partition assignment mechanism assign consecutive partitions to the same ShuffleServer to reduce the frequency of getShuffleResult.

    It can be enabled by the following config
      ```bash
        # Default value is ROUND, it will poll to allocate partitions to ShuffleServer
        rss.coordinator.select.partition.strategy CONTINUOUS
        
        # Default value is 1.0, used to estimate task concurrency, how likely is this part of the resource between spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors to be allocated
        --conf spark.rss.estimate.task.concurrency.dynamic.factor=1.0
        ```
### Deploy MapReduce Client Plugin

1. Add client jar to the classpath of each NodeManager, e.g., <HADOOP>/share/hadoop/mapreduce/

The jar for MapReduce is located in <RSS_HOME>/jars/client/mr/rss-client-mr-XXXXX-shaded.jar

2. Update MapReduce conf to enable Uniffle, eg,

   ```
   -Dmapreduce.rss.coordinator.quorum=<coordinatorIp1>:19999,<coordinatorIp2>:19999
   -Dyarn.app.mapreduce.am.command-opts=org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster
   -Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.RssMapOutputCollector
   -Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.RssShuffle
   ```
Note that the RssMRAppMaster will automatically disable slow start (i.e., `mapreduce.job.reduce.slowstart.completedmaps=1`)
and job recovery (i.e., `yarn.app.mapreduce.am.job.recovery.enable=false`)


## Configuration

The important configuration of client is listed as following.

### Common Setting
These configurations are shared by all types of clients.

|Property Name|Default|Description|
|---|---|---|
|<client_type>.rss.coordinator.quorum|-|Coordinator quorum|
|<client_type>.rss.writer.buffer.size|3m|Buffer size for single partition data|
|<client_type>.rss.storage.type|-|Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS|
|<client_type>.rss.client.read.buffer.size|14m|The max data size read from storage|
|<client_type>.rss.client.send.threadPool.size|5|The thread size for send shuffle data to shuffle server|
|<client_type>.rss.client.assignment.tags|-|The comma-separated list of tags for deciding assignment shuffle servers. Notice that the SHUFFLE_SERVER_VERSION will always as the assignment tag whether this conf is set or not|
|<client_type>.rss.client.data.commit.pool.size|The number of assigned shuffle servers|The thread size for sending commit to shuffle servers|
|<client_type>.rss.client.assignment.shuffle.nodes.max|-1|The number of required assignment shuffle servers. If it is less than 0 or equals to 0 or greater than the coordinator's config of "rss.coordinator.shuffle.nodes.max", it will use the size of "rss.coordinator.shuffle.nodes.max" default|
|<client_type>.rss.client.io.compression.codec|lz4|The compression codec is used to compress the shuffle data. Default codec is `lz4`. Other options are`ZSTD` and `SNAPPY`.|
|<client_type>.rss.client.io.compression.zstd.level|3|The zstd compression level, the default level is 3|
|<client_type>.rss.client.shuffle.data.distribution.type|NORMAL|The type of partition shuffle data distribution, including normal and local_order. The default value is normal. Now this config is only valid in Spark3.x|
|<client_type>.rss.estimate.task.concurrency.dynamic.factor|1.0|Between 0 and 1, used to estimate task concurrency, when the client is spark, it represents how likely is this part of the resource between spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors to be allocated, when the client is mr, it represents how likely the resources of map and reduce are satisfied. Effective when <client_type>.rss.estimate.server.assignment.enabled=true or Coordinator's rss.coordinator.select.partition.strategy is CONTINUOUS.|
|<client_type>.rss.estimate.server.assignment.enabled|false|Support mr and spark, whether to enable estimation of the number of ShuffleServers that need to be allocated based on the number of concurrent tasks.|
|<client_type>.rss.estimate.task.concurrency.per.server|80|It takes effect when rss.estimate.server.assignment.enabled=true, how many tasks are concurrently assigned to a ShuffleServer.|
Notice:

1. `<client_type>` should be `spark` or `mapreduce`

2. `<client_type>.rss.coordinator.quorum` is compulsory, and other configurations are optional when coordinator dynamic configuration is enabled.

### Adaptive Remote Shuffle Enabling 

To select build-in shuffle or remote shuffle in a smart manner, Uniffle support adaptive enabling. 
The client should use `DelegationRssShuffleManager` and provide its unique <access_id> so that the coordinator could distinguish whether it should enable remote shuffle.

```
spark.shuffle.manager org.apache.spark.shuffle.DelegationRssShuffleManager
spark.rss.access.id=<access_id> 
```

Notice:
Currently, this feature only supports Spark. 

Other configuration:

|Property Name|Default|Description|
|---|---|---|
|spark.rss.access.timeout.ms|10000|The timeout to access Uniffle coordinator|
|spark.rss.client.access.retry.interval.ms|20000|The interval between retries fallback to SortShuffleManager|
|spark.rss.client.access.retry.times|0|The number of retries fallback to SortShuffleManager|
  

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

### Spark Specialized Setting

The important configuration is listed as following.

|Property Name|Default|Description|
|---|---|---|
|spark.rss.writer.buffer.spill.size|128m|Buffer size for total partition data|
|spark.rss.client.send.size.limit|16m|The max data size sent to shuffle server|
|spark.rss.client.unregister.thread.pool.size|10|The max size of thread pool of unregistering|
|spark.rss.client.unregister.request.timeout.sec|10|The max timeout sec when doing unregister to remote shuffle-servers|


### MapReduce Specialized Setting

|Property Name|Default|Description|
|---|---|---|
|mapreduce.rss.client.max.buffer.size|3k|The max buffer size in map side|
|mapreduce.rss.client.batch.trigger.num|50|The max batch of buffers to send data in map side|




### Remote Spill (Experimental)

In cloud environment, VM may have very limited disk space and performance.
This experimental feature allows reduce tasks to spill data to remote storage (e.g., hdfs)

|Property Name|Default|Description|
|---|---|---|
|mapreduce.rss.reduce.remote.spill.enable|false|Whether to use remote spill|
|mapreduce.rss.reduce.remote.spill.attempt.inc|1|Increase reduce attempts as hdfs is easier to crash than disk|
|mapreduce.rss.reduce.remote.spill.replication|1|The replication number to spill data to hdfs|
|mapreduce.rss.reduce.remote.spill.retries|5|The retry number to spill data to hdfs|

Notice: this feature requires the MEMORY_LOCAL_HDFS mode.
 