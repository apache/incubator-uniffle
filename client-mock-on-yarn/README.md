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

# Uniffle Mock Client on Yarn - Usage Guide

## Parameter Description

| Parameter Name                                | Default Value | Description                                                                                                                                               |
|-----------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uniffle.mock.client.serverId`                | None          | Uniffle server ID                                                                                                                                         |
| `uniffle.mock.client.container.num`           | 3             | Number of containers to start in the Yarn application, which corresponds to the number of concurrent client processes                                     |
| `uniffle.mock.client.threadCount`             | 1             | Number of concurrent threads running in each container process. The actual number of working threads is `threadCount + 1` when each thread is concurrent. |
| `uniffle.mock.client.queueName`               | default       | Yarn resource queue name                                                                                                                                  |
| `uniffle.mock.client.jarPath.list`            | None          | HDFS addresses of additional JARs or other resources to download to AM or Task local, separated by commas (e.g., HDFS address of RSS shaded JAR)          |
| `uniffle.mock.client.tmp.hdfs.path`           | None          | A writable HDFS address for uploading temporary application resources                                                                                     |
| `uniffle.mock.client.shuffleCount`            | 1             | Number of shuffles included in a single `sendShuffleData` request                                                                                         |
| `uniffle.mock.client.partitionCount`          | 1             | Number of partitions included in each shuffle of a single `sendShuffleData` request                                                                       |
| `uniffle.mock.client.blockCount`              | 1             | Number of blocks included in each partition of a single `sendShuffleData` request                                                                         |
| `uniffle.mock.client.blockSize`               | 1024          | Size of each block in a single `sendShuffleData` request                                                                                                  |
| `uniffle.mock.client.am.vCores`               | 8             | Number of virtual cores specified when requesting the Application Master (AM)                                                                             |
| `uniffle.mock.client.am.memory`               | 4096          | Memory size (in MB) specified when requesting the AM                                                                                                      |
| `uniffle.mock.client.container.vCores`        | 2             | Number of virtual cores specified when requesting task containers                                                                                         |
| `uniffle.mock.client.container.memory`        | 2048          | Memory size (in MB) specified when requesting task containers                                                                                             |
| `uniffle.mock.client.am.jvm.opts`             | None          | Additional JVM options for debugging when the execution result is abnormal                                                                                |
| `uniffle.mock.client.container.jvm.opts`      | None          | Additional JVM options for debugging when the execution result is abnormal                                                                                |

## Running Example

1. Change to the Hadoop directory and execute the test on Yarn program:

```bash
cd $HADOOP_HOME
```

2. Execute the example command:

```bash
$ bin/yarn jar uniffle-mock-client-1.0-SNAPSHOT.jar \
-Duniffle.mock.client.serverId=<UNIFFLE_SERVER_ID> \
-Duniffle.mock.client.container.num=1000 \
-Duniffle.mock.client.queueName=<YOUR_QUEUE_NAME>  \
-Duniffle.mock.client.jarPath.list=hdfs://ns1/tmp/rss-client-spark3-shaded.jar  \
-Duniffle.mock.client.tmp.hdfs.path=hdfs://ns1/user/xx/tmp/uniffle-mock-client/  \
-Duniffle.mock.client.shuffleCount=5 \
-Duniffle.mock.client.partitionCount=50 \
-Duniffle.mock.client.blockCount=100 \
-Duniffle.mock.client.blockSize=10240 \
-Duniffle.mock.client.threadCount=10
```

3. Example Output:

```plaintext
24/12/30 15:03:47 INFO client.UniffleMockClientOnYarnClient: appId: application_1729845342052_5295913
...
Application killed: application_1729845342052_5295913
Application status: KILLED
```

This guide provides a comprehensive overview of how to run the Uniffle Mock Client on Yarn, including parameter descriptions and a step-by-step example. Adjust the parameters as needed for your specific use case.
