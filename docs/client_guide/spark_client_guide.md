---
layout: page
displayTitle: Deploy Spark Client Plugin & Configurations
title: Deploy Spark Client Plugin & Configurations
description: Deploy Spark Client Plugin & Configurations
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
# Deploy Spark Client Plugin & Configurations
## Deploy Spark Client Plugin

1. Add client jar to Spark classpath, e.g. SPARK_HOME/jars/

   The jar for Spark2 is located in <RSS_HOME>/jars/client/spark2/rss-client-spark2-shaded-${version}.jar

   The jar for Spark3 is located in <RSS_HOME>/jars/client/spark3/rss-client-spark3-shaded-${version}.jar

2. Update Spark conf to enable Uniffle, e.g.

   ```
   # Uniffle transmits serialized shuffle data over network, therefore a serializer that supports relocation of
   # serialized object should be used. 
   spark.serializer org.apache.spark.serializer.KryoSerializer # this could also be in the spark-defaults.conf
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
   It will be enabled by default when AQE is enabled.

2. Continuous partition assignment mechanism assign consecutive partitions to the same ShuffleServer to reduce the frequency of getShuffleResult.

    It can be enabled by the following config
      ```bash
        # Default value is ROUND, it will poll to allocate partitions to ShuffleServer
        rss.coordinator.select.partition.strategy CONTINUOUS
        
        # Default value is 1.0, used to estimate task concurrency, how likely is this part of the resource between spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors to be allocated
        --conf spark.rss.estimate.task.concurrency.dynamic.factor=1.0
      ```

Since v0.8.0, `RssShuffleManager` would disable local shuffle reader(`set spark.sql.adaptive.localShuffleReader.enabled=false`) optimization by default.

Local shuffle reader as its name indicates is suitable and optimized for spark's external shuffle service, and shall not be used for remote shuffle service. It would cause many random small IOs and network connections with Uniffle's shuffle server


## Spark Specific Configurations

The important configuration is listed as following.

| Property Name                                         | Default | Description                                                                                                                                                                    |
|-------------------------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.rss.writer.buffer.spill.size                    | 128m    | Buffer size for total partition data                                                                                                                                           |
| spark.rss.client.send.size.limit                      | 16m     | The max data size sent to shuffle server                                                                                                                                       |
| spark.rss.client.unregister.thread.pool.size          | 10      | The max size of thread pool of unregistering                                                                                                                                   |
| spark.rss.client.unregister.request.timeout.sec       | 10      | The max timeout sec when doing unregister to remote shuffle-servers                                                                                                            |
| spark.rss.client.off.heap.memory.enable               | false   | The client use off heap memory to process data                                                                                                                                 |
| spark.rss.client.remote.storage.useLocalConfAsDefault | false   | This option is only valid when the remote storage path is specified. If ture, the remote storage conf will use the client side hadoop configuration loaded from the classpath  |

### Adaptive Remote Shuffle Enabling 
Currently, this feature only supports Spark. 

To select build-in shuffle or remote shuffle in a smart manner, Uniffle support adaptive enabling. 
The client should use `DelegationRssShuffleManager` and provide its unique <access_id> so that the coordinator could distinguish whether it should enable remote shuffle.

```
spark.shuffle.manager org.apache.spark.shuffle.DelegationRssShuffleManager
spark.rss.access.id=<access_id> 
```


Other configuration:

|Property Name|Default|Description|
|---|---|---|
|spark.rss.access.timeout.ms|10000|The timeout to access Uniffle coordinator|
|spark.rss.client.access.retry.interval.ms|20000|The interval between retries fallback to SortShuffleManager|
|spark.rss.client.access.retry.times|0|The number of retries fallback to SortShuffleManager|