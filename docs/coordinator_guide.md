---
layout: page
displayTitle: Uniffle Coordinator Guide
title: Uniffle Coordinator Guide
description: Uniffle Coordinator Guide
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

# Uniffle Coordinator Guide

Uniffle is a unified remote shuffle service for compute engines, the role of coordinator is responsibility for
collecting status of shuffle server and doing the assignment for the job.

## Deploy
This document will introduce how to deploy Uniffle coordinators.

### Steps
1. unzip package to RSS_HOME
2. update RSS_HOME/bin/rss-env.sh, eg,
   ```
     JAVA_HOME=<java_home>
     HADOOP_HOME=<hadoop home>
     XMX_SIZE="16g"
   ```
3. update RSS_HOME/conf/coordinator.conf, eg,
   ```
     rss.rpc.server.port 19999
     rss.jetty.http.port 19998
     rss.coordinator.server.heartbeat.timeout 30000
     rss.coordinator.app.expired 60000
     rss.coordinator.shuffle.nodes.max 5
     # enable dynamicClientConf, and coordinator will be responsible for most of client conf
     rss.coordinator.dynamicClientConf.enabled true
     # config the path of client conf
     rss.coordinator.dynamicClientConf.path <RSS_HOME>/conf/dynamic_client.conf
     # config the path of excluded shuffle server
     rss.coordinator.exclude.nodes.file.path <RSS_HOME>/conf/exclude_nodes
   ```
4. update <RSS_HOME>/conf/dynamic_client.conf, rss client will get default conf from coordinator eg,
   ```
    # MEMORY_LOCALFILE_HDFS is recommandation for production environment
    rss.storage.type MEMORY_LOCALFILE_HDFS
    # multiple remote storages are supported, and client will get assignment from coordinator
    rss.coordinator.remote.storage.path hdfs://cluster1/path,hdfs://cluster2/path
    rss.writer.require.memory.retryMax 1200
    rss.client.retry.max 100
    rss.writer.send.check.timeout 600000
    rss.client.read.buffer.size 14m
   ```
   
5. update <RSS_HOME>/conf/exclude_nodes, coordinator will update excluded node by this file eg,
   ```
    # shuffleServer's ip and port, connected with "-"
    110.23.15.36-19999
    110.23.15.35-19996
   ```

6. start Coordinator
   ```
    bash RSS_HOME/bin/start-coordnator.sh
   ```

## Configuration

### Common settings
|Property Name|Default|	Description|
|---|---|---|
|rss.coordinator.server.heartbeat.timeout|30000|Timeout if can't get heartbeat from shuffle server|
|rss.coordinator.server.periodic.output.interval.times|30|The periodic interval times of output alive nodes. The interval sec can be calculated by (rss.coordinator.server.heartbeat.timeout/3 * rss.coordinator.server.periodic.output.interval.times). Default output interval is 5min.|
|rss.coordinator.assignment.strategy|PARTITION_BALANCE|Strategy for assigning shuffle server, PARTITION_BALANCE should be used for workload balance|
|rss.coordinator.app.expired|60000|Application expired time (ms), the heartbeat interval should be less than it|
|rss.coordinator.shuffle.nodes.max|9|The max number of shuffle server when do the assignment|
|rss.coordinator.dynamicClientConf.path|-|The path of configuration file which have default conf for rss client|
|rss.coordinator.exclude.nodes.file.path|-|The path of configuration file which have exclude nodes|
|rss.coordinator.exclude.nodes.check.interval.ms|60000|Update interval (ms) for exclude nodes|
|rss.coordinator.access.checkers|org.apache.uniffle.coordinator.AccessClusterLoadChecker|The access checkers will be used when the spark client use the DelegationShuffleManager, which will decide whether to use rss according to the result of the specified access checkers|
|rss.coordinator.access.loadChecker.memory.percentage|15.0|The minimal percentage of available memory percentage of a server|
|rss.coordinator.dynamicClientConf.enabled|false|whether to enable dynamic client conf, which will be fetched by spark client|
|rss.coordinator.dynamicClientConf.path|-|The dynamic client conf of this cluster and can be stored in HDFS or local|
|rss.coordinator.dynamicClientConf.updateIntervalSec|120|The dynamic client conf update interval in seconds|
|rss.coordinator.remote.storage.cluster.conf|-|Remote Storage Cluster related conf with format $clusterId,$key=$value, separated by ';'|
|rss.rpc.server.port|-|RPC port for coordinator|
|rss.jetty.http.port|-|Http port for coordinator|
|rss.coordinator.remote.storage.select.strategy|APP_BALANCE|Strategy for selecting the remote path|
|rss.coordinator.remote.storage.schedule.time|60000|The time of scheduling the read and write time of the paths to obtain different HDFS|
|rss.coordinator.remote.storage.schedule.file.size|204800000|The size of the file that the scheduled thread reads and writes|
|rss.coordinator.remote.storage.schedule.access.times|3|The number of times to read and write HDFS files|

### AccessClusterLoadChecker settings
|Property Name|Default|	Description|
|---|---|---|
|rss.coordinator.access.loadChecker.serverNum.threshold|-|The minimal required number of healthy shuffle servers when being accessed by client|

### AccessCandidatesChecker settings
AccessCandidatesChecker is one of the built-in access checker, which will allow user to define the candidates list to use rss.  

|Property Name|Default|	Description|
|---|---|---|
|rss.coordinator.access.candidates.updateIntervalSec|120|Accessed candidates update interval in seconds, which is only valid when AccessCandidatesChecker is enabled.|
|rss.coordinator.access.candidates.path|-|Accessed candidates file path, the file can be stored on HDFS|
