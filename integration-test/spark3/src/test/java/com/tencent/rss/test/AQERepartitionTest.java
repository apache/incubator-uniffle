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

package com.tencent.rss.test;


import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.Dataset;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertTrue;

public class AQERepartitionTest extends SparkIntegrationTestBase {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateCommonSparkConf(SparkConf sparkConf) {
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "true");
    sparkConf.set(SQLConf.COALESCE_PARTITIONS_ENABLED(), "true");
    sparkConf.set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM().key(), "10");
    sparkConf.set(SQLConf.SHUFFLE_PARTITIONS().key(), "10");
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, "HDFS");
    sparkConf.set(RssClientConfig.RSS_BASE_PATH, HDFS_URI + "rss/test");
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    Thread.sleep(4000);
    List<Column> repartitionCols = Lists.newArrayList();
    repartitionCols.add(new Column("id"));
    Dataset<Long> df = spark.range(10).repartition(
        JavaConverters.asScalaBuffer(repartitionCols).toList());
    Long[][] result = (Long[][])df.rdd().collectPartitions();
    Map<Integer, List<Long>> map = Maps.newHashMap();
    for (int i = 0; i < result.length; i++) {
      map.putIfAbsent(i, Lists.newArrayList());
      for (int j = 0; j < result[i].length; j++) {
        map.get(i).add(result[i][j]);
      }
    }
    for (int i = 0; i < result.length; i++) {
      map.get(i).sort(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return Long.compare(o1, o2);
        }
      });
    }
    assertTrue(result.length < 10);
    return map;
  }
}
