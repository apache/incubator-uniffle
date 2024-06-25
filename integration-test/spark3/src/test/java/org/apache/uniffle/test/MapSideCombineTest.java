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

package org.apache.uniffle.test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.test.listener.WriteAndReadMetricsSparkListener;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapSideCombineTest extends SparkIntegrationTestBase {

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf grpcShuffleServerConf = getShuffleServerConf(ServerType.GRPC);
    createShuffleServer(grpcShuffleServerConf);
    ShuffleServerConf nettyShuffleServerConf = getShuffleServerConf(ServerType.GRPC_NETTY);
    createShuffleServer(nettyShuffleServerConf);
    startServers();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    sparkConf.set(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    sparkConf.set("spark." + RssSparkConfig.RSS_CLIENT_MAP_SIDE_COMBINE_ENABLED.key(), "true");
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    WriteAndReadMetricsSparkListener listener = new WriteAndReadMetricsSparkListener();
    spark.sparkContext().addSparkListener(listener);

    Thread.sleep(4000);
    Map<String, Object> result = Maps.newHashMap();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    List<Integer> data = Stream.iterate(1, n -> n + 1).limit(1000).collect(Collectors.toList());
    sc.parallelize(data, 10).mapToPair(x -> new Tuple2<>(x % 10, 1)).reduceByKey((x, y) -> x + y)
        .collect().stream()
        .forEach(x -> result.put(x._1 + "-result-value", x._2));

    spark.sparkContext().listenerBus().waitUntilEmpty();

    for (int stageId : spark.sparkContext().statusTracker().getJobInfo(0).get().stageIds()) {
      long writeRecords = listener.getWriteRecords(stageId);
      long readRecords = listener.getReadRecords(stageId);
      result.put(stageId + "-write-records", writeRecords);
      result.put(stageId + "-read-records", readRecords);
    }

    // check map side combine
    assertEquals(100L, result.get("0-write-records"));

    return result;
  }
}
