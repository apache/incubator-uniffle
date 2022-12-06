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

package org.apache.uniffle.coordinator;

import io.prometheus.client.CollectorRegistry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.metrics.JvmMetrics;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.security.SecurityConfig;
import org.apache.uniffle.common.security.SecurityContextFactory;
import org.apache.uniffle.common.web.CommonMetricsServlet;
import org.apache.uniffle.common.web.JettyServer;
import org.apache.uniffle.coordinator.metric.CoordinatorGrpcMetrics;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategy;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategyFactory;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KRB5_CONF_FILE;

/**
 * The main entrance of coordinator service
 */
public class CoordinatorServer {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

  private final CoordinatorConf coordinatorConf;
  private JettyServer jettyServer;
  private ServerInterface server;
  private ClusterManager clusterManager;
  private AssignmentStrategy assignmentStrategy;
  private ClientConfManager clientConfManager;
  private AccessManager accessManager;
  private ApplicationManager applicationManager;
  private GRPCMetrics grpcMetrics;

  public CoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    this.coordinatorConf = coordinatorConf;
    try {
      initialization();
    } catch (Exception e) {
      LOG.error("Errors on initializing coordinator server.", e);
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init coordinator server using config {}", configFile);

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);

    // Start the coordinator service
    final CoordinatorServer coordinatorServer = new CoordinatorServer(coordinatorConf);

    coordinatorServer.start();
    coordinatorServer.blockUntilShutdown();
  }

  public void start() throws Exception {
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
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
    if (clusterManager != null) {
      clusterManager.close();
    }
    if (accessManager != null) {
      accessManager.close();
    }
    if (clientConfManager != null) {
      clientConfManager.close();
    }
    SecurityContextFactory.get().getSecurityContext().close();
    server.stop();
  }

  private void initialization() throws Exception {
    jettyServer = new JettyServer(coordinatorConf);
    // register metrics first to avoid NPE problem when add dynamic metrics
    registerMetrics();
    this.applicationManager = new ApplicationManager(coordinatorConf);

    SecurityConfig securityConfig = null;
    if (coordinatorConf.getBoolean(RSS_SECURITY_HADOOP_KERBEROS_ENABLE)) {
      securityConfig = SecurityConfig.newBuilder()
          .krb5ConfPath(coordinatorConf.getString(RSS_SECURITY_HADOOP_KRB5_CONF_FILE))
          .keytabFilePath(coordinatorConf.getString(RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE))
          .principal(coordinatorConf.getString(RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL))
          .reloginIntervalSec(coordinatorConf.getLong(RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC))
          .build();
    }
    SecurityContextFactory.get().init(securityConfig);

    // load default hadoop configuration
    Configuration hadoopConf = new Configuration();
    ClusterManagerFactory clusterManagerFactory = new ClusterManagerFactory(coordinatorConf, hadoopConf);

    this.clusterManager = clusterManagerFactory.getClusterManager();
    this.clientConfManager = new ClientConfManager(coordinatorConf, hadoopConf, applicationManager);
    AssignmentStrategyFactory assignmentStrategyFactory =
        new AssignmentStrategyFactory(coordinatorConf, clusterManager);
    this.assignmentStrategy = assignmentStrategyFactory.getAssignmentStrategy();
    this.accessManager = new AccessManager(coordinatorConf, clusterManager,
        applicationManager.getQuotaManager(), hadoopConf);
    CoordinatorFactory coordinatorFactory = new CoordinatorFactory(this);
    server = coordinatorFactory.getServer();
  }

  private void registerMetrics() {
    LOG.info("Register metrics");
    CollectorRegistry coordinatorCollectorRegistry = new CollectorRegistry(true);
    CoordinatorMetrics.register(coordinatorCollectorRegistry);
    grpcMetrics = new CoordinatorGrpcMetrics();
    grpcMetrics.register(new CollectorRegistry(true));
    boolean verbose = coordinatorConf.getBoolean(CoordinatorConf.RSS_JVM_METRICS_VERBOSE_ENABLE);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry, verbose);

    LOG.info("Add metrics servlet");
    jettyServer.addServlet(
        new CommonMetricsServlet(CoordinatorMetrics.getCollectorRegistry()),
        "/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(grpcMetrics.getCollectorRegistry()),
        "/metrics/grpc");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry()),
        "/metrics/jvm");
    jettyServer.addServlet(
        new CommonMetricsServlet(CoordinatorMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(grpcMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/grpc");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/jvm");
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    return assignmentStrategy;
  }

  public CoordinatorConf getCoordinatorConf() {
    return coordinatorConf;
  }

  public ApplicationManager getApplicationManager() {
    return applicationManager;
  }

  public AccessManager getAccessManager() {
    return accessManager;
  }

  public ClientConfManager getClientConfManager() {
    return clientConfManager;
  }

  public GRPCMetrics getGrpcMetrics() {
    return grpcMetrics;
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }
}
