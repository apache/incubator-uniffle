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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Sets;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import com.tencent.rss.common.Arguments;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.metrics.GRPCMetrics;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.rpc.ServerInterface;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.common.web.CommonMetricsServlet;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.server.buffer.ShuffleBufferManager;
import com.tencent.rss.storage.util.StorageType;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);
  private RegisterHeartBeat registerHeartBeat;
  private String id;
  private String ip;
  private int port;
  private ShuffleServerConf shuffleServerConf;
  private JettyServer jettyServer;
  private ShuffleTaskManager shuffleTaskManager;
  private ServerInterface server;
  private ShuffleFlushManager shuffleFlushManager;
  private ShuffleBufferManager shuffleBufferManager;
  private MultiStorageManager multiStorageManager;
  private HealthCheck healthCheck;
  private Set<String> tags = Sets.newHashSet();
  private AtomicBoolean isHealthy = new AtomicBoolean(true);
  private GRPCMetrics grpcMetrics;

  public ShuffleServer(ShuffleServerConf shuffleServerConf) throws Exception {
    this.shuffleServerConf = shuffleServerConf;
    initialization();
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init shuffle server using config {}", configFile);

    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(configFile);
    final ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();

    shuffleServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    registerHeartBeat.startHeartBeat();
    jettyServer.start();
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("*** shutting down gRPC server since JVM is shutting down");
        try {
          stopServer();
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
        LOG.info("*** server shut down");
      }
    });
    LOG.info("Shuffle server start successfully!");
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
      LOG.info("Jetty Server Stopped!");
    }
    if (registerHeartBeat != null) {
      registerHeartBeat.shutdown();
      LOG.info("HeartBeat Stopped!");
    }
    if (multiStorageManager != null) {
      multiStorageManager.stop();
      LOG.info("MultiStorage Stopped!");
    }
    if (healthCheck != null) {
      healthCheck.stop();
      LOG.info("HealthCheck stopped!");
    }
    server.stop();
    LOG.info("RPC Server Stopped!");
  }

  private void initialization() throws Exception {
    ip = RssUtils.getHostIp();
    if (ip == null) {
      throw new RuntimeException("Couldn't acquire host Ip");
    }
    port = shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT);
    id = ip + "-" + port;
    LOG.info("Start to initialize server {}", id);
    jettyServer = new JettyServer(shuffleServerConf);
    registerMetrics();

    boolean useMultiStorage = shuffleServerConf.getBoolean(ShuffleServerConf.MULTI_STORAGE_ENABLE);
    String storageType = shuffleServerConf.getString(RssBaseConf.RSS_STORAGE_TYPE);
    if (StorageType.LOCALFILE_AND_HDFS.name().equals(storageType)) {
      useMultiStorage = true;
      shuffleServerConf.setBoolean(ShuffleServerConf.MULTI_STORAGE_ENABLE, true);
      LOG.warn("StorageType LOCALFILE_HDFS will enable multistorage function");
    }
    if (useMultiStorage && !StorageType.LOCALFILE_AND_HDFS.name().equals(storageType)) {
      throw new IllegalArgumentException("Only StorageType LOCALFILE_AND_HDFS support multiStorage function");
    }
    if (useMultiStorage) {
      multiStorageManager = new MultiStorageManager(shuffleServerConf, id);
      multiStorageManager.start();
    }

    boolean healthCheckEnable = shuffleServerConf.getBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE);
    if (healthCheckEnable) {
      healthCheck = new HealthCheck(isHealthy, shuffleServerConf);
      healthCheck.start();
    }

    registerHeartBeat = new RegisterHeartBeat(this);
    shuffleFlushManager = new ShuffleFlushManager(shuffleServerConf, id, this, multiStorageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleServerConf, shuffleFlushManager);
    shuffleTaskManager = new ShuffleTaskManager(shuffleServerConf, shuffleFlushManager,
        shuffleBufferManager, multiStorageManager);

    ShuffleServerFactory shuffleServerFactory = new ShuffleServerFactory(this);
    server = shuffleServerFactory.getServer();

    // it's the system tag for server's version
    tags.add(Constants.SHUFFLE_SERVER_VERSION);
  }

  private void registerMetrics() {
    LOG.info("Register metrics");
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    grpcMetrics = new ShuffleServerGrpcMetrics();
    grpcMetrics.register(new CollectorRegistry(true));
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    boolean verbose = shuffleServerConf.getBoolean(ShuffleServerConf.RSS_JVM_METRICS_VERBOSE_ENABLE);
    JvmMetrics.register(jvmCollectorRegistry, verbose);

    LOG.info("Add metrics servlet");
    jettyServer.addServlet(
        new CommonMetricsServlet(ShuffleServerMetrics.getCollectorRegistry()),
        "/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(grpcMetrics.getCollectorRegistry()),
        "/metrics/grpc");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry()),
        "/metrics/jvm");
    jettyServer.addServlet(
        new CommonMetricsServlet(ShuffleServerMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(grpcMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/grpc");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/jvm");
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }

  public String getIp() {
    return this.ip;
  }

  public String getId() {
    return this.id;
  }

  public int getPort() {
    return this.port;
  }

  public ShuffleServerConf getShuffleServerConf() {
    return this.shuffleServerConf;
  }

  public ServerInterface getServer() {
    return server;
  }

  public void setServer(ServerInterface server) {
    this.server = server;
  }

  public ShuffleTaskManager getShuffleTaskManager() {
    return shuffleTaskManager;
  }

  public ShuffleFlushManager getShuffleFlushManager() {
    return shuffleFlushManager;
  }

  public long getUsedMemory() {
    return shuffleBufferManager.getUsedMemory();
  }

  public long getPreAllocatedMemory() {
    return shuffleBufferManager.getPreAllocatedSize();
  }

  public long getAvailableMemory() {
    return shuffleBufferManager.getCapacity() - shuffleBufferManager.getUsedMemory();
  }

  public int getEventNumInFlush() {
    return shuffleFlushManager.getEventNumInFlush();
  }

  public ShuffleBufferManager getShuffleBufferManager() {
    return shuffleBufferManager;
  }

  public MultiStorageManager getMultiStorageManager() {
    return multiStorageManager;
  }

  public boolean isMultiStorageEnabled() {
    return multiStorageManager != null;
  }

  public Set<String> getTags() {
    return tags;
  }

  public boolean isHealthy() {
    return isHealthy.get();
  }

  public GRPCMetrics getGrpcMetrics() {
    return grpcMetrics;
  }
}
