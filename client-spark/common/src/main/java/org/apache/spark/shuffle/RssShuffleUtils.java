/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static byte[] compressDataOrigin(CompressionCodec compressionCodec, byte[] data) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
    OutputStream os = compressionCodec.compressedOutputStream(baos);
    try {
      os.write(data);
      os.flush();
    } catch (Exception e) {
      LOG.error("Fail to compress shuffle data", e);
      throw new RuntimeException(e);
    } finally {
      try {
        os.close();
      } catch (Exception e) {
        LOG.warn("Can't close compression output stream, resource leak", e);
      }
    }
    return baos.toByteArray();
  }

  public static byte[] decompressDataOrigin(CompressionCodec compressionCodec, byte[] data, int compressionBlockSize) {
    if (data == null || data.length == 0) {
      LOG.warn("Empty data is found when do decompress");
      return null;
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    InputStream is = compressionCodec.compressedInputStream(bais);
    List<byte[]> dataBlocks = Lists.newArrayList();
    int readSize = 0;
    int lastReadSize = 0;
    boolean shouldEnd = false;
    do {
      byte[] block = new byte[compressionBlockSize];
      lastReadSize = readSize;
      try {
        readSize = is.read(block, 0, compressionBlockSize);
      } catch (Exception e) {
        LOG.error("Fail to decompress shuffle data", e);
        throw new RuntimeException(e);
      }
      if (readSize > -1) {
        dataBlocks.add(block);
      }
      if (shouldEnd && readSize > -1) {
        String errorMsg = "Fail to decompress shuffle data, it may be caused by incorrect compression buffer";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
      if (readSize < compressionBlockSize) {
        shouldEnd = true;
      }
    } while (readSize > -1);
    int uncompressLength = (dataBlocks.size() - 1) * compressionBlockSize + lastReadSize;
    byte[] uncompressData = new byte[uncompressLength];
    for (int i = 0; i < dataBlocks.size() - 1; i++) {
      System.arraycopy(dataBlocks.get(i), 0, uncompressData,
          i * compressionBlockSize, compressionBlockSize);
    }
    byte[] lastBlock = dataBlocks.get(dataBlocks.size() - 1);
    System.arraycopy(lastBlock, 0,
        uncompressData, (dataBlocks.size() - 1) * compressionBlockSize, lastReadSize);
    return uncompressData;
  }

  public static byte[] compressData(byte[] data) {
    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    return compressor.compress(data);
  }

  public static byte[] decompressData(byte[] data, int uncompressLength) {
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    byte[] uncompressData = new byte[uncompressLength];
    fastDecompressor.decompress(data, 0, uncompressData, 0, uncompressLength);
    return uncompressData;
  }

  public static ByteBuffer decompressData(ByteBuffer data, int uncompressLength) {
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    ByteBuffer uncompressData = ByteBuffer.allocateDirect(uncompressLength);
    fastDecompressor.decompress(data, data.position(), uncompressData, 0, uncompressLength);
    return uncompressData;
  }

  public static Configuration newHadoopConfiguration(SparkConf sparkConf) {
    SparkHadoopUtil util = new SparkHadoopUtil();
    Configuration conf = util.newConfiguration(sparkConf);

    boolean useOdfs = sparkConf.getBoolean(RssClientConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE,
        RssClientConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE_DEFAULT_VALUE);
    if (useOdfs) {
      final int OZONE_PREFIX_LEN = "spark.rss.ozone.".length();
      conf.setBoolean(RssClientConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.substring(OZONE_PREFIX_LEN), useOdfs);
      conf.set(
          RssClientConfig.RSS_OZONE_FS_HDFS_IMPL.substring(OZONE_PREFIX_LEN),
          sparkConf.get(RssClientConfig.RSS_OZONE_FS_HDFS_IMPL, RssClientConfig.RSS_OZONE_FS_HDFS_IMPL_DEFAULT_VALUE));
      conf.set(
          RssClientConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL.substring(OZONE_PREFIX_LEN),
          sparkConf.get(
              RssClientConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL,
              RssClientConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL_DEFAULT_VALUE));
    }

    return conf;

  }

  public static String getSparkVersion() {
    return package$.MODULE$.SPARK_VERSION();
  }
}
