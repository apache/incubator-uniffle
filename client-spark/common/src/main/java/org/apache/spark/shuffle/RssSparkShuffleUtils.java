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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;

public class RssSparkShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssSparkShuffleUtils.class);

  public static Configuration newHadoopConfiguration(SparkConf sparkConf) {
    SparkHadoopUtil util = new SparkHadoopUtil();
    Configuration conf = util.newConfiguration(sparkConf);

    boolean useOdfs = sparkConf.getBoolean(RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE,
        RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE_DEFAULT_VALUE);
    if (useOdfs) {
      final int OZONE_PREFIX_LEN = "spark.rss.ozone.".length();
      conf.setBoolean(RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.substring(OZONE_PREFIX_LEN), useOdfs);
      conf.set(
          RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL.substring(OZONE_PREFIX_LEN),
          sparkConf.get(RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL, RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL_DEFAULT_VALUE));
      conf.set(
          RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL.substring(OZONE_PREFIX_LEN),
          sparkConf.get(
              RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL,
              RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL_DEFAULT_VALUE));
    }

    return conf;
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
    String clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE,
        RssSparkConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM);
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
      String sparkConfKey = kv.getKey();
      if (!sparkConfKey.startsWith(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX)) {
        sparkConfKey = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + sparkConfKey;
      }
      String confVal = kv.getValue();
      if (!sparkConf.contains(sparkConfKey) || RssSparkConfig.RSS_MANDATORY_CLUSTER_CONF.contains(sparkConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", sparkConfKey, confVal);
        sparkConf.set(sparkConfKey, confVal);
      }
    }
  }

  public static void validateRssClientConf(SparkConf sparkConf) {
    String msgFormat = "%s must be set by the client or fetched from coordinators.";
    if (!sparkConf.contains(RssSparkConfig.RSS_STORAGE_TYPE)) {
      String msg = String.format(msgFormat, "Storage type");
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }
}
