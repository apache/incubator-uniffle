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

package com.tencent.rss.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.rss.common.metrics.TestUtils;
import com.tencent.rss.storage.util.StorageType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class ShuffleServerMetricsTest {

  private static final String SERVER_METRICS_URL = "http://127.0.0.1:12345/metrics/server";
  private static final String SERVER_JVM_URL = "http://127.0.0.1:12345/metrics/jvm";
  private static final String SERVER_GRPC_URL = "http://127.0.0.1:12345/metrics/grpc";
  private static ShuffleServer shuffleServer;

  @BeforeClass
  public static void setUp() throws Exception {
    ShuffleServerConf ssc = new ShuffleServerConf();
    ssc.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    ssc.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 128);
    ssc.set(ShuffleServerConf.RPC_SERVER_PORT, 12346);
    ssc.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "tmp");
    ssc.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    ssc.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "fake.coordinator:123");
    ssc.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 1000L);
    ssc.set(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE, 100);
    ssc.set(ShuffleServerConf.SERVER_BUFFER_SPILL_THRESHOLD, 200L);
    shuffleServer = new ShuffleServer(ssc);
    shuffleServer.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    shuffleServer.stopServer();
  }

  @Test
  public void testJvmMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_JVM_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
  }

  @Test
  public void testServerMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(26, actualObj.get("metrics").size());
  }

  @Test
  public void testGrpcMetrics() throws Exception {
    String content = TestUtils.httpGetMetrics(SERVER_GRPC_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(22, actualObj.get("metrics").size());
  }

  @Test
  public void testServerMetricsConcurrently() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    List<Callable<Void>> calls = new ArrayList<>();
    ShuffleServerMetrics.gaugeBufferDataSize.set(0);

    long expectedNum = 0;
    for (int i = 1; i < 5; ++i) {
      int cur = i * i;
      if (i % 2 == 0) {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.gaugeBufferDataSize.inc(cur);
            return null;
          }
        });
        expectedNum += cur;
      } else {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.gaugeBufferDataSize.dec(cur);
            return null;
          }
        });
        expectedNum -= cur;
      }
    }

    List<Future<Void>> results = executorService.invokeAll(calls);
    for (Future f : results) {
      f.get();
    }

    String content = TestUtils.httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);

    final long tmp = expectedNum;
    actualObj.get("metrics").iterator().forEachRemaining(jsonNode -> {
      String name = jsonNode.get("name").textValue();
      if (name.equals("buffered_data_size")) {
        assertEquals(tmp, jsonNode.get("value").asLong());
      }
    });
  }
}
