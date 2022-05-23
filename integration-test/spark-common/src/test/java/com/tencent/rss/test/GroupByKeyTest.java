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

import com.google.common.collect.Lists;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

public class GroupByKeyTest extends SimpleTestBase {

  @Test
  public void groupByTest() throws Exception {
    run();
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    // take a rest to make sure shuffle server is registered
    Thread.sleep(3000);
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    JavaPairRDD<String, String> javaPairRDD1 = jsc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("a", "1"), new Tuple2<>("b", "2"),
        new Tuple2<>("c", "3"), new Tuple2<>("d", "4")), 2);
    JavaPairRDD<String, Iterable<String>> javaPairRDD = javaPairRDD1.groupByKey().sortByKey();
    return javaPairRDD.collectAsMap();
  }
}
