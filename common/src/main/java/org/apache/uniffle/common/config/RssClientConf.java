package org.apache.uniffle.common.config;

import org.apache.uniffle.common.compression.CompressionFactory;

import static org.apache.uniffle.common.compression.CompressionFactory.Type.ZSTD;

public class RssClientConf {

  public static final ConfigOption<CompressionFactory.Type> COMPRESSION_TYPE = ConfigOptions
      .key("rss.client.io.compression.codec")
      .enumType(CompressionFactory.Type.class)
      .defaultValue(ZSTD)
      .withDescription("");

  public static final ConfigOption<Boolean> LZ4_COMPRESSION_DIRECT_MEMORY_ENABLED = ConfigOptions
      .key("rss.client.io.compression.lz4.direct.memory.enable")
      .booleanType()
      .defaultValue(true)
      .withDescription("");

  public static final ConfigOption<Integer> ZSTD_COMPRESSION_LEVEL = ConfigOptions
      .key("rss.client.io.compression.zstd.level")
      .intType()
      .defaultValue(1)
      .withDescription("");
}
