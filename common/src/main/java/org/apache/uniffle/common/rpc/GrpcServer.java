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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class GrpcServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private final Server server;
  private final int port;
  private int listenPort;
  private final ExecutorService pool;

  protected GrpcServer(
      RssBaseConf conf,
      List<Pair<BindableService, List<ServerInterceptor>>> servicesWithInterceptors,
      GRPCMetrics grpcMetrics) {
    this.port = conf.getInteger(RssBaseConf.RPC_SERVER_PORT);
    long maxInboundMessageSize = conf.getLong(RssBaseConf.RPC_MESSAGE_MAX_SIZE);
    int rpcExecutorSize = conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE);
    pool = new GrpcThreadPoolExecutor(
        rpcExecutorSize,
        rpcExecutorSize * 2,
        10,
        TimeUnit.MINUTES,
        Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
        ThreadUtils.getThreadFactory("Grpc"),
        grpcMetrics
    );

    boolean isMetricsEnabled = conf.getBoolean(RssBaseConf.RPC_METRICS_ENABLED);
    ServerBuilder<?> builder = ServerBuilder
        .forPort(port)
        .executor(pool)
        .maxInboundMessageSize((int)maxInboundMessageSize);
    if (isMetricsEnabled) {
      builder.addTransportFilter(new MonitoringServerTransportFilter(grpcMetrics));
    }
    servicesWithInterceptors.forEach((serviceWithInterceptors) -> {
      List<ServerInterceptor> interceptors = serviceWithInterceptors.getRight();
      if (isMetricsEnabled) {
        MonitoringServerInterceptor monitoringInterceptor =
            new MonitoringServerInterceptor(grpcMetrics);
        List<ServerInterceptor> newInterceptors = Lists.newArrayList(interceptors);
        newInterceptors.add(monitoringInterceptor);
        interceptors = newInterceptors;
      }
      builder.addService(ServerInterceptors.intercept(serviceWithInterceptors.getLeft(), interceptors));
    });
    this.server = builder.build();
  }

  public static class Builder {

    private RssBaseConf rssBaseConf;
    private GRPCMetrics grpcMetrics;

    private List<Pair<BindableService, List<ServerInterceptor>>> servicesWithInterceptors = new ArrayList<>();

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder conf(RssBaseConf rssBaseConf) {
      this.rssBaseConf = rssBaseConf;
      return this;
    }

    public Builder addService(BindableService bindableService, ServerInterceptor... interceptors) {
      this.servicesWithInterceptors.add(Pair.of(bindableService, Lists.newArrayList(interceptors)));
      return this;
    }

    public Builder grpcMetrics(GRPCMetrics metrics) {
      this.grpcMetrics = metrics;
      return this;
    }

    public GrpcServer build() {
      return new GrpcServer(rssBaseConf, servicesWithInterceptors, grpcMetrics);
    }
  }

  public static class GrpcThreadPoolExecutor extends ThreadPoolExecutor {
    private final GRPCMetrics grpcMetrics;
    private final AtomicLong activeThreadSize = new AtomicLong(0L);

    public GrpcThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        GRPCMetrics grpcMetrics) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.grpcMetrics = grpcMetrics;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      grpcMetrics.incGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY);
      grpcMetrics.setGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY,
          getQueue().size());
      super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      grpcMetrics.decGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY);
      grpcMetrics.setGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY,
          getQueue().size());
      super.afterExecute(r, t);
    }
  }

  public void start() throws IOException {
    try {
      server.start();
      listenPort = server.getPort();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Fail to start grpc server", e, LOG);
    }
    LOG.info("Grpc server started, configured port: {}, listening on {}.", port, listenPort);
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
      LOG.info("GRPC server stopped!");
    }
    if (pool != null) {
      pool.shutdown();
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public int getPort() {
    return port <= 0 ? listenPort : port;
  }

}
