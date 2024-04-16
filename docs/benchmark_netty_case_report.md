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

## Environment

### Software

Uniffle 0.9.0, Hadoop 2.8.5, Spark 3.3.1

### Hardware

#### Uniffle Cluster

| Cluster Type | Memory | CPU Cores | Disk Configuration for Every Shuffle Server | Max IO Read/Write Speed | Quantity                              | Network Bandwidth |
|--------------|--------|-----------|---------------------------------------------|-------------------------|---------------------------------------|-------------------|
| HDD          | 250G   | 96        | 10 * 4T HDD                                 | 150MB/s                 | 2 * Coordinator + 10 * Shuffle Server | 25GB/s            |
| SSD          | 250G   | 96        | 1 * 6T NVME                                 | 3GB/s                   | 2 * Coordinator + 10 * Shuffle Server | 25GB/s            |

#### Hadoop Yarn Cluster

2 * ResourceManager + 750 * NodeManager, every machine 12 * 4T HDD

## Configuration

Spark's configuration:

  ````
  spark.speculation false
  spark.executor.instances 1400
  spark.executor.cores 2
  spark.executor.memory 20g
  spark.executor.memoryOverhead 1024
  spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
  spark.sql.shuffle.partitions 20000
  spark.sql.files.maxPartitionBytes 107374182
  spark.rss.storage.type MEMORY_LOCALFILE
  spark.rss.writer.buffer.spill.size 1g
  spark.rss.writer.buffer.size 16m
  spark.rss.client.send.size.limit 32m
  spark.rss.client.rpc.maxAttempts 50
  spark.rss.resubmit.stage false
  # Enable Netty mode
  spark.rss.client.type GRPC_NETTY
  spark.rss.client.netty.io.mode EPOLL
  ````

Shuffle Server's configuration:

  ````
  rss.storage.type MEMORY_LOCALFILE
  rss.server.buffer.capacity 140g
  rss.server.read.buffer.capacity 20g
  rss.rpc.executor.size 1000
  # Enable Netty mode
  rss.rpc.server.type GRPC_NETTY
  rss.server.netty.epoll.enable true
  rss.server.netty.port 17000
  rss.server.netty.connect.backlog 128
  ````

## TPC-DS(SF=40000)

We use [spark-sql-perf](https://github.com/databricks/spark-sql-perf) to generate 10TB data.

We use the following special SQL to perform stress testing, it mainly focuses on shuffle, with no data skewness, and has
no practical business implications:

````
select SUM(IFNULL(CAST(ss_sold_time_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_item_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_cdemo_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_hdemo_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_addr_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_store_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_promo_sk AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_ticket_number AS DECIMAL(10, 2)), 0) + IFNULL(CAST(ss_quantity AS DECIMAL(10, 2)), 0) + IFNULL(ss_wholesale_cost, 0) + IFNULL(ss_list_price, 0) + IFNULL(ss_sales_price, 0) + IFNULL(ss_ext_discount_amt, 0) + IFNULL(ss_ext_sales_price, 0) + IFNULL(ss_ext_wholesale_cost, 0) + IFNULL(ss_ext_list_price, 0) + IFNULL(ss_ext_tax, 0) + IFNULL(ss_coupon_amt, 0) + IFNULL(ss_net_paid, 0) + IFNULL(ss_net_paid_inc_tax, 0) + IFNULL(ss_net_profit, 0)) as sum_all_fields from (select * from (select s.*,c.* from (select *,floor(rand(123)*82857000) as sr from store_sales) s join (select*,floor(rand(123)*82857000)as cr from customer)  c on s.sr=c.cr) sc DISTRIBUTE BY sc.ss_customer_sk,sc.ss_item_sk)
````

## Read-Write Performance

Total: Read 10.7TiB, Write 6.4TiB

| Concurrent Tasks | Type          | Single Shuffle Server Write Speed | Single Shuffle Server Read Speed | Tasks Total Time | E2E Time | Netty(SSD) Speedup | Netty(SSD) Total Task Time Reduction | Notes                                                                                                                                                                                                                                |
|------------------|---------------|-----------------------------------|----------------------------------|------------------|----------|--------------------|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1400             | Netty(SSD)    | 0.93GB/s                          | 1.56GB/s                         | 268.7h           | 12min    | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | gRPC(SSD)     | 0.75GB/s                          | 1.25GB/s                         | 330.4h           | 15min    | 123.02%            | 18.67%                               |                                                                                                                                                                                                                                      |
|                  | Netty(HDD)    | 0.24GB/s                          | 0.4GB/s                          | 1024.4h          | 46min    | 381.12%            | 73.77%                               |                                                                                                                                                                                                                                      |
|                  | Spark ESS     | 0.5GB/s                           | 0.82GB/s                         | 525.5h           | 23min    | 195.56%            | 48.88%                               |                                                                                                                                                                                                                                      |
|                  | Vanilla Spark | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |
| 2800             | Netty(SSD)    | 1.02GB/s                          | 1.70GB/s                         | 450.7h           | 11min    | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | gRPC(SSD)     | 0.86GB/s                          | 1.44GB/s                         | 566.4h           | 13min    | 125.64%            | 20.42%                               |                                                                                                                                                                                                                                      |
|                  | Netty(HDD)    | 0.24GB/s                          | 0.4GB/s                          | 2009.9h          | 46min    | 445.83%            | 77.6%                                |                                                                                                                                                                                                                                      |
|                  | Spark ESS     | 0.5GB/s                           | 0.68GB/s                         | 672.3h           | 23min    | 149.19%            | 32.96%                               |                                                                                                                                                                                                                                      |
|                  | Vanilla Spark | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |
| 5600             | Netty(SSD)    | 1.02GB/s                          | 1.70GB/s                         | 896.2h           | 11min    | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | gRPC(SSD)     | 0.80GB/s                          | 1.34GB/s                         | 1145.1h          | 14min    | 127.74%            | 21.72%                               |                                                                                                                                                                                                                                      |
|                  | Netty(HDD)    | 0.22GB/s                          | 0.36GB/s                         | 4671.3h          | 52min    | 520.98%            | 80.8%                                |                                                                                                                                                                                                                                      |
|                  | Spark ESS     | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | Vanilla Spark | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |
| 11200            | Netty(SSD)    | 0.86GB/s                          | 1.44GB/s                         | 1783.1h          | 13min    | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | gRPC(SSD)     | 0.62GB/s                          | 1.04GB/s                         | 2028.2h          | 15min    | 113.74%            | 12.08%                               | At a concurrency of 11,200, the Shuffle Server becomes very unstable in gRPC mode compared to Netty mode, with higher memory usage and CPU load. It is highly susceptible to encountering OOM issues and is not recommended for use. |
|                  | Netty(HDD)    | 0.20GB/s                          | 0.34GB/s                         | 8716.5h          | 54min    | 488.61%            | 79.5%                                |                                                                                                                                                                                                                                      |
|                  | Spark ESS     | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |
|                  | Vanilla Spark | -                                 | -                                | __*Failed*__     | -        | -                  | -                                    |                                                                                                                                                                                                                                      |

Note:

1. Read and write operations are essentially happening simultaneously.
2. The calculation formula for `Netty(SSD) Total Task Time Reduction` is as follows:

````
Netty(SSD) Total Task Time Reduction = (Tasks Total Time - Tasks Total Time( Netty(SSD) )) / Tasks Total Time * 100%
````

3. The calculation formula for `Netty(SSD) Speedup` is as follows:

````
Netty(SSD) Speedup = Tasks Total Time / Tasks Total Time( Netty(SSD) ) * 100%
````

## Conclusion

We can draw the following conclusions:

1. At 1400 concurrency, Vanilla Spark is already incapable of successfully completing tasks, and at 5600 concurrency,
   Spark
   ESS also fails to complete tasks. However, whether it is HDD or SSD, and whether it is gRPC mode or Netty mode,
   Uniffle can all run normally. **Uniffle can significantly improve job stability in high-pressure scenarios**.
2. When comparing using SSDs, **Netty mode brings about a 20% of total task time reduction compared to gRPC mode**.
3. When comparing with Netty mode turned on, **SSD brings about an 80% of total task time reduction compared to HDD**.
4. **Above 11200 concurrency, it is not recommended to use gRPC mode**, as gRPC mode will cause the machine's load
   to be much higher than Netty mode, and the Shuffle Server's process will consume more memory on the machine.
   Also, it is highly susceptible to encountering OOM issues.
   See https://github.com/apache/incubator-uniffle/issues/1651 for more details.