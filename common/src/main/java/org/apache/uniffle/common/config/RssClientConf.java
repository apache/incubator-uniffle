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

import org.apache.uniffle.common.compression.CompressionFactory;

import static org.apache.uniffle.common.compression.CompressionFactory.Type.ZSTD;

public class RssClientConf {

  public static final ConfigOption<CompressionFactory.Type> COMPRESSION_TYPE = ConfigOptions
      .key("rss.client.io.compression.codec")
      .enumType(CompressionFactory.Type.class)
      .defaultValue(ZSTD)
      .withDescription("The compression codec is used to compress the shuffle data. "
          + "Default codec is `zstd`, `lz4` also can be used.");

  public static final ConfigOption<Integer> ZSTD_COMPRESSION_LEVEL = ConfigOptions
      .key("rss.client.io.compression.zstd.level")
      .intType()
      .defaultValue(3)
      .withDescription("The zstd compression level, the default level is 3");
}
