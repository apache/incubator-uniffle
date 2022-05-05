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
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;

public class RssSparkShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssSparkShuffleUtils.class);

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

  public static ShuffleManager loadShuffleManager(String name, SparkConf conf, boolean isDriver) throws Exception {
    Class<?> klass = Class.forName(name);
    Constructor<?> constructor;
    ShuffleManager instance;
    try {
      constructor = klass.getConstructor(conf.getClass(), Boolean.TYPE);
      instance = (ShuffleManager) constructor.newInstance(conf, isDriver);
    } catch (NoSuchMethodException e) {
      constructor = klass.getConstructor(conf.getClass());
      instance = (ShuffleManager) constructor.newInstance(conf);
    }
    return instance;
  }

  public static List<CoordinatorClient> createCoordinatorClients(SparkConf sparkConf) throws RuntimeException {
    String clientType = sparkConf.get(RssClientConfig.RSS_CLIENT_TYPE,
        RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    String coordinators = sparkConf.get(RssClientConfig.RSS_COORDINATOR_QUORUM);
    CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(clientType);
    return coordinatorClientFactory.createCoordinatorClient(coordinators);
  }

  public static void applyDynamicClientConf(SparkConf sparkConf, Map<String, String> confItems) {
    if (sparkConf == null) {
      LOG.warn("Spark conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String confKey = kv.getKey();
      String confVal = kv.getValue();
      if (!sparkConf.contains(confKey) || RssClientConfig.RSS_MANDATORY_CLUSTER_CONF.contains(confKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", confKey, confVal);
        sparkConf.set(confKey, confVal);
      }
    }
  }

  public static void validateRssClientConf(SparkConf sparkConf) {
    String msgFormat = "%s must be set by the client or fetched from coordinators.";
    if (!sparkConf.contains(RssClientConfig.RSS_STORAGE_TYPE)) {
      String msg = String.format(msgFormat, "Storage type");
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }
}
