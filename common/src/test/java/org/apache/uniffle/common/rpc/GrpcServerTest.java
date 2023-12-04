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

package org.apache.uniffle.common.rpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Queues;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.proto.ShuffleManagerGrpc;

import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;
import static org.apache.uniffle.common.metrics.GRPCMetrics.GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY;
import static org.apache.uniffle.common.metrics.GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GrpcServerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServerTest.class);

  @Test
  public void testGrpcExecutorPool() throws Exception {
    // Explicitly setting the synchronizing variable as false at the beginning of test run
    RssConf rssConf = new RssConf();
    GrpcServer.reset();
    GRPCMetrics grpcMetrics = GRPCMetrics.getEmptyGRPCMetrics(rssConf);
    grpcMetrics.register(new CollectorRegistry(true));
    GrpcServer.GrpcThreadPoolExecutor executor =
        new GrpcServer.GrpcThreadPoolExecutor(
            2,
            2,
            100,
            TimeUnit.MINUTES,
            Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
            ThreadUtils.getThreadFactory("Grpc"),
            grpcMetrics);

    CountDownLatch countDownLatch = new CountDownLatch(3);
    for (int i = 0; i < 3; i++) {
      final int index = i;
      executor.submit(
          () -> {
            try {
              Thread.sleep(100 * 2);
            } catch (InterruptedException interruptedException) {
              interruptedException.printStackTrace();
            }
            LOGGER.info("Finished task: {}", index);
            countDownLatch.countDown();
          });
    }

    while (!GrpcServer.isPoolExecutorHasExecuted()) {
      Thread.yield();
    }
    Thread.sleep(120);
    executor.correctMetrics();
    double activeThreads =
        grpcMetrics.getGaugeMap().get(GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY).get();
    assertEquals(2, activeThreads);
    double queueSize =
        grpcMetrics.getGaugeMap().get(GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY).get();
    assertEquals(1, queueSize);

    countDownLatch.await();
    // the metrics is updated afterExecute, which means it may take a while for the thread to
    // decrease the metrics
    Thread.sleep(100);
    activeThreads = grpcMetrics.getGaugeMap().get(GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY).get();
    assertEquals(0, activeThreads);
    queueSize = grpcMetrics.getGaugeMap().get(GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY).get();
    assertEquals(0, queueSize);

    executor.shutdown();
  }

  @Test
  public void testRandomPort() throws Exception {
    RssConf rssConf = new RssConf();
    GRPCMetrics grpcMetrics = GRPCMetrics.getEmptyGRPCMetrics(rssConf);
    grpcMetrics.register(new CollectorRegistry(true));
    RssBaseConf conf = new RssBaseConf();
    conf.set(RPC_SERVER_PORT, 0);
    GrpcServer server =
        GrpcServer.Builder.newBuilder()
            .conf(conf)
            .grpcMetrics(grpcMetrics)
            .addService(new ShuffleManagerGrpc.ShuffleManagerImplBase() {})
            .build();
    server.start();
    assertTrue(server.getPort() > 0);
  }
}
