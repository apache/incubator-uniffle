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
     rss.server.flush.threadPool.size 10
     rss.server.buffer.capacity 40g
     rss.server.read.buffer.capacity 20g
     rss.server.heartbeat.timeout 60000
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
|Property Name|Default|Description|
|---|---|---|
|rss.coordinator.quorum|-|Coordinator quorum|
|rss.rpc.server.port|-|RPC port for Shuffle server|
|rss.jetty.http.port|-|Http port for Shuffle server|
|rss.server.buffer.capacity|-|Max memory of buffer manager for shuffle server|
|rss.server.memory.shuffle.highWaterMark.percentage|75.0|Threshold of spill data to storage, percentage of rss.server.buffer.capacity|
|rss.server.memory.shuffle.lowWaterMark.percentage|25.0|Threshold of keep data in memory, percentage of rss.server.buffer.capacity|
|rss.server.read.buffer.capacity|-|Max size of buffer for reading data|
|rss.server.heartbeat.interval|10000|Heartbeat interval to Coordinator (ms)|
|rss.server.flush.threadPool.size|10|Thread pool for flush data to file|
|rss.server.commit.timeout|600000|Timeout when commit shuffle data (ms)|
|rss.storage.type|-|Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS|
|rss.server.flush.cold.storage.threshold.size|64M| The threshold of data size for LOACALFILE and HDFS if MEMORY_LOCALFILE_HDFS is used|
|rss.server.tags|-|The comma-separated list of tags to indicate the shuffle server's attributes. It will be used as the assignment basis for the coordinator|
|rss.server.single.buffer.flush.enabled|false|Whether single buffer flush when size exceeded rss.server.single.buffer.flush.threshold|
|rss.server.single.buffer.flush.threshold|64M|The threshold of single shuffle buffer flush|
|rss.server.disk.capacity|-1|Disk capacity that shuffle server can use. If it's negative, it will use the default disk whole space|
|rss.server.multistorage.fallback.strategy.class|-|The fallback strategy for `MEMORY_LOCALFILE_HDFS`. Support `org.apache.uniffle.server.storage.RotateStorageManagerFallbackStrategy`,`org.apache.uniffle.server.storage.LocalStorageManagerFallbackStrategy` and `org.apache.uniffle.server.storage.HdfsStorageManagerFallbackStrategy`. If not set, `org.apache.uniffle.server.storage.HdfsStorageManagerFallbackStrategy` will be used.|
|rss.server.leak.shuffledata.check.interval|3600000|The interval of leak shuffle data check (ms)|
