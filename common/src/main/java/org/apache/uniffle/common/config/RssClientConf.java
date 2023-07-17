/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.config;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.netty.IOMode;

import static org.apache.uniffle.common.compression.Codec.Type.LZ4;

public class RssClientConf {

  public static final ConfigOption<Codec.Type> COMPRESSION_TYPE =
      ConfigOptions.key("rss.client.io.compression.codec")
          .enumType(Codec.Type.class)
          .defaultValue(LZ4)
          .withDescription(
              "The compression codec is used to compress the shuffle data. "
                  + "Default codec is `LZ4`. Other options are`ZSTD` and `SNAPPY`.");

  public static final ConfigOption<Integer> ZSTD_COMPRESSION_LEVEL =
      ConfigOptions.key("rss.client.io.compression.zstd.level")
          .intType()
          .defaultValue(3)
          .withDescription("The zstd compression level, the default level is 3");

  public static final ConfigOption<ShuffleDataDistributionType> DATA_DISTRIBUTION_TYPE =
      ConfigOptions.key("rss.client.shuffle.data.distribution.type")
          .enumType(ShuffleDataDistributionType.class)
          .defaultValue(ShuffleDataDistributionType.NORMAL)
          .withDescription(
              "The type of partition shuffle data distribution, including normal and local_order. "
                  + "The default value is normal. This config is only valid in Spark3.x");

  public static final ConfigOption<Integer> MAX_CONCURRENCY_PER_PARTITION_TO_WRITE =
      ConfigOptions.key("rss.client.max.concurrency.of.per-partition.write")
          .intType()
          .defaultValue(0)
          .withDescription(
              "The max concurrency for single partition to write, the value is the max file number "
                  + "for one partition, remote shuffle server should respect this.");

  public static final ConfigOption<Integer> NETTY_IO_CONNECT_TIMEOUT_MS =
      ConfigOptions.key("rss.client.netty.io.connect.timeout.ms")
          .intType()
          .defaultValue(10 * 1000)
          .withDescription("netty connect to server time out mills");

  public static final ConfigOption<IOMode> NETTY_IO_MODE =
      ConfigOptions.key("rss.client.netty.io.mode")
          .enumType(IOMode.class)
          .defaultValue(IOMode.NIO)
          .withDescription("Netty EventLoopGroup backend, available options: NIO, EPOLL.");

  public static final ConfigOption<Integer> NETTY_IO_CONNECTION_TIMEOUT_MS =
      ConfigOptions.key("rss.client.netty.client.connection.timeout.ms")
          .intType()
          .defaultValue(10 * 60 * 1000)
          .withDescription("connection active timeout");

  public static final ConfigOption<Integer> NETTY_CLIENT_THREADS =
      ConfigOptions.key("rss.client.netty.client.threads")
          .intType()
          .defaultValue(0)
          .withDescription("Number of threads used in the client thread pool.");

  public static final ConfigOption<Boolean> NETWORK_CLIENT_PREFER_DIRECT_BUFS =
      ConfigOptions.key("rss.client.netty.client.prefer.direct.bufs")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "If true, we will prefer allocating off-heap byte buffers within Netty.");

  public static final ConfigOption<Integer> NETTY_CLIENT_NUM_CONNECTIONS_PER_PEER =
      ConfigOptions.key("rss.client.netty.client.connections.per.peer")
          .intType()
          .defaultValue(2)
          .withDescription("Number of concurrent connections between two nodes.");

  public static final ConfigOption<Integer> NETTY_CLIENT_RECEIVE_BUFFER =
      ConfigOptions.key("rss.client.netty.client.receive.buffer")
          .intType()
          .defaultValue(0)
          .withDescription(
              "Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer "
                  + "should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps "
                  + "buffer size should be ~ 1.25MB.");

  public static final ConfigOption<Integer> NETTY_CLIENT_SEND_BUFFER =
      ConfigOptions.key("rss.client.netty.client.send.buffer")
          .intType()
          .defaultValue(0)
          .withDescription("Send buffer size (SO_SNDBUF).");

  // this is reversed for internal settings, and should never set by user.
  public static final ConfigOption<Integer> SHUFFLE_MANAGER_GRPC_PORT =
      ConfigOptions.key("rss.shuffle.manager.grpc.port")
          .intType()
          .noDefaultValue()
          .withDescription(
              "internal configuration to indicate which port is actually bind for shuffle manager service.");

  public static final ConfigOption<Boolean> OFF_HEAP_MEMORY_ENABLE =
      ConfigOptions.key("rss.client.off.heap.memory.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Client can use off heap memory");

  public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT =
      ConfigOptions.key("rss.index.read.limit").intType().defaultValue(500);

  public static final ConfigOption<String> RSS_STORAGE_TYPE =
      ConfigOptions.key("rss.storage.type")
          .stringType()
          .defaultValue("")
          .withDescription("Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS");

  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
      ConfigOptions.key("rss.client.read.buffer.size")
          .stringType()
          .defaultValue("14m")
          .withDescription("The max data size read from storage");
}
