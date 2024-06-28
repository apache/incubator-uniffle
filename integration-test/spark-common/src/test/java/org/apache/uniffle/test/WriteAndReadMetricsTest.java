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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.test.listener.WriteAndReadMetricsSparkListener;

public class WriteAndReadMetricsTest extends SimpleTestBase {

  @Test
  public void test() throws Exception {
    run();
  }

  @Override
  public Map<String, Long> runTest(SparkSession spark, String fileName) throws Exception {
    // Instantiate WriteAndReadMetricsSparkListener and add it to SparkContext
    WriteAndReadMetricsSparkListener listener = new WriteAndReadMetricsSparkListener();
    spark.sparkContext().addSparkListener(listener);

    // take a rest to make sure shuffle server is registered
    Thread.sleep(3000);

    Dataset<Row> df1 =
        spark
            .range(0, 100, 1, 10)
            .select(
                functions
                    .when(functions.col("id").$less$eq(50), 1)
                    .otherwise(functions.col("id"))
                    .as("key1"),
                functions.col("id").as("value1"));
    df1.createOrReplaceTempView("table1");

    List<?> list = spark.sql("select count(value1) from table1 group by key1").collectAsList();
    Map<String, Long> result = new HashMap<>();
    result.put("size", (long) list.size());

    // take a rest to make sure all task metrics are updated before read stageData
    Thread.sleep(100);

    for (int stageId : spark.sparkContext().statusTracker().getJobInfo(0).get().stageIds()) {
      long writeRecords = listener.getWriteRecords(stageId);
      long readRecords = listener.getReadRecords(stageId);
      result.put(stageId + "-write-records", writeRecords);
      result.put(stageId + "-read-records", readRecords);
    }

    return result;
  }
}
