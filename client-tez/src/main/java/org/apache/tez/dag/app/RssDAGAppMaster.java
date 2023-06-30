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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.Loader;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezClassLoader;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.log4j.LogManager.CONFIGURATOR_CLASS_KEY;
import static org.apache.log4j.LogManager.DEFAULT_CONFIGURATION_KEY;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT;

public class RssDAGAppMaster extends DAGAppMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RssDAGAppMaster.class);
  private ShuffleWriteClient shuffleWriteClient;
  private TezRemoteShuffleManager tezRemoteShuffleManager;
  private Map<String, String> clusterClientConf;

  final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
          ThreadUtils.getThreadFactory("AppHeartbeat")
  );

  public RssDAGAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId,
          String nmHost, int nmPort, int nmHttpPort, Clock clock, long appSubmitTime, boolean isSession,
          String workingDirectory, String[] localDirs, String[] logDirs, String clientVersion,
          Credentials credentials, String jobUserName, AMPluginDescriptorProto pluginDescriptorProto) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime, isSession,
            workingDirectory, localDirs, logDirs, clientVersion, credentials, jobUserName, pluginDescriptorProto);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    initAndStartRSSClient(this, conf);
  }

  public ShuffleWriteClient getShuffleWriteClient() {
    return shuffleWriteClient;
  }

  public void setShuffleWriteClient(ShuffleWriteClient shuffleWriteClient) {
    this.shuffleWriteClient = shuffleWriteClient;
  }

  public TezRemoteShuffleManager getTezRemoteShuffleManager() {
    return tezRemoteShuffleManager;
  }

  public void setTezRemoteShuffleManager(TezRemoteShuffleManager tezRemoteShuffleManager) {
    this.tezRemoteShuffleManager = tezRemoteShuffleManager;
  }

  public Map<String, String> getClusterClientConf() {
    return clusterClientConf;
  }

  /**
   * Init and Start Rss Client
   * @param appMaster
   * @param conf
   * @throws Exception
   */
  public static void initAndStartRSSClient(final RssDAGAppMaster appMaster, Configuration conf) throws Exception {

    ShuffleWriteClient client = RssTezUtils.createShuffleClient(conf);
    appMaster.setShuffleWriteClient(client);

    String coordinators = conf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);

    String strAppAttemptId = appMaster.getAttemptID().toString();
    long heartbeatInterval = conf.getLong(RssTezConfig.RSS_HEARTBEAT_INTERVAL,
            RssTezConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    long heartbeatTimeout = conf.getLong(RssTezConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    client.registerApplicationInfo(strAppAttemptId, heartbeatTimeout, "user");

    appMaster.scheduledExecutorService.scheduleAtFixedRate(
            () -> {
              try {
                client.sendAppHeartbeat(strAppAttemptId, heartbeatTimeout);
                LOG.debug("Finish send heartbeat to coordinator and servers");
              } catch (Exception e) {
                LOG.warn("Fail to send heartbeat to coordinator and servers", e);
              }
            },
            heartbeatInterval / 2,
            heartbeatInterval,
            TimeUnit.MILLISECONDS);

    appMaster.setTezRemoteShuffleManager(new TezRemoteShuffleManager(strAppAttemptId, null, conf,
            strAppAttemptId, client));
    appMaster.getTezRemoteShuffleManager().initialize();
    appMaster.getTezRemoteShuffleManager().start();

    // apply dynamic configuration
    boolean dynamicConfEnabled = conf.getBoolean(RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
        RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);
    if (dynamicConfEnabled) {
      appMaster.clusterClientConf = client.fetchClientConf(
          conf.getInt(RssTezConfig.RSS_ACCESS_TIMEOUT_MS, RssTezConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE));
    }

    mayCloseTezSlowStart(conf);
  }

  @Override
  protected DAG createDAG(DAGProtos.DAGPlan dagPB) {
    DAGImpl dag = createDAG(dagPB, null);
    registerStateEnteredCallback(dag, this);
    return dag;
  }

  /**
   * main method
   * @param args
   */
  public static void main(String[] args) {
    try {
      // We use trick way to introduce RssDAGAppMaster by the config tez.am.launch.cmd-opts.
      // It means some property which is set by command line will be ingored, so we must reload it.
      boolean sessionModeCliOption = false;
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("-D")) {
          String[] property = args[i].split("=");
          if (property.length < 2) {
            System.setProperty(property[0].substring(2), "");
          } else {
            System.setProperty(property[0].substring(2), property[1]);
          }
        } else if (args[i].contains("--session") || args[i].contains("-s")) {
          sessionModeCliOption = true;
        }
      }
      // Load the log4j config is only init in static code block of LogManager, so we must reconfigure.
      reconfigureLog4j();

      // Install the tez class loader, which can be used add new resources
      TezClassLoader.setupTezClassLoader();
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      final String pid = System.getenv().get("JVM_PID");
      String containerIdStr =
              System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String appSubmitTimeStr =
              System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      Objects.requireNonNull(appSubmitTimeStr,
              ApplicationConstants.APP_SUBMIT_TIME_ENV + " is null");

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();

      String jobUserName = System
              .getenv(ApplicationConstants.Environment.USER.name());

      LOG.info("Creating RssDAGAppMaster for "
              + "applicationId=" + applicationAttemptId.getApplicationId()
              + ", attemptNum=" + applicationAttemptId.getAttemptId()
              + ", AMContainerId=" + containerId
              + ", jvmPid=" + pid
              + ", userFromEnv=" + jobUserName
              + ", cliSessionOption=" + sessionModeCliOption
              + ", pwd=" + System.getenv(ApplicationConstants.Environment.PWD.name())
              + ", localDirs=" + System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())
              + ", logDirs=" + System.getenv(ApplicationConstants.Environment.LOG_DIRS.name()));

      Configuration conf = new Configuration(new YarnConfiguration());

      DAGProtos.ConfigurationProto confProto = TezUtilsInternal.readUserSpecifiedTezConfiguration(
              System.getenv(ApplicationConstants.Environment.PWD.name()));
      TezUtilsInternal.addUserSpecifiedTezConfiguration(conf, confProto.getConfKeyValuesList());

      AMPluginDescriptorProto amPluginDescriptorProto = null;
      if (confProto.hasAmPluginDescriptor()) {
        amPluginDescriptorProto = confProto.getAmPluginDescriptor();
      }

      UserGroupInformation.setConfiguration(conf);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

      TezUtilsInternal.setSecurityUtilConfigration(LOG, conf);

      String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
              System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      RssDAGAppMaster appMaster =
              new RssDAGAppMaster(applicationAttemptId, containerId, nodeHostString,
                      Integer.parseInt(nodePortString),
                      Integer.parseInt(nodeHttpPortString), new SystemClock(), appSubmitTime,
                      sessionModeCliOption,
                      System.getenv(ApplicationConstants.Environment.PWD.name()),
                      TezCommonUtils.getTrimmedStrings(
                              System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())),
                      TezCommonUtils.getTrimmedStrings(System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
                      clientVersion, credentials, jobUserName, amPluginDescriptorProto);
      ShutdownHookManager.get().addShutdownHook(
              new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
      ShutdownHookManager.get().addShutdownHook(new RssDAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);


      // log the system properties
      if (LOG.isInfoEnabled()) {
        String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(conf);
        if (systemPropsToLog != null) {
          LOG.info(systemPropsToLog);
        }
      }

      initAndStartAppMaster(appMaster, conf);
    } catch (Throwable t) {
      LOG.error("Error starting RssDAGAppMaster", t);
      System.exit(1);
    }
  }

  static void mayCloseTezSlowStart(Configuration conf) {
    if (!conf.getBoolean(RssTezConfig.RSS_AM_SLOW_START_ENABLE, RssTezConfig.RSS_AM_SLOW_START_ENABLE_DEFAULT)) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 1.0f);
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 1.0f);
    }
  }

  static class RssDAGAppMasterShutdownHook implements Runnable {
    RssDAGAppMaster appMaster;

    RssDAGAppMasterShutdownHook(RssDAGAppMaster appMaster) {
      this.appMaster = appMaster;
    }

    @Override
    public void run() {
      if (appMaster.shuffleWriteClient != null) {
        appMaster.shuffleWriteClient.close();
      }

      if (appMaster.tezRemoteShuffleManager != null) {
        try {
          appMaster.tezRemoteShuffleManager.shutdown();
        } catch (Exception e) {
          RssDAGAppMaster.LOG.info("TezRemoteShuffleManager shutdown error: " + e.getMessage());
        }
      }

      RssDAGAppMaster.LOG.info(
          "RssDAGAppMaster received a signal. Signaling RMCommunicator and JobHistoryEventHandler.");
      this.appMaster.stop();
    }
  }

  @VisibleForTesting
  public static void registerStateEnteredCallback(DAGImpl dag, RssDAGAppMaster appMaster) {
    StateMachineTez stateMachine = (StateMachineTez) getPrivateField(dag, "stateMachine");
    stateMachine.registerStateEnteredCallback(DAGState.INITED, new DagInitialCallback(appMaster));
  }

  static class DagInitialCallback implements OnStateChangedCallback<DAGState, DAGImpl> {

    private RssDAGAppMaster appMaster;
    
    DagInitialCallback(RssDAGAppMaster appMaster) {
      this.appMaster = appMaster;
    } 

    @Override
    public void onStateChanged(DAGImpl dag, DAGState dagState) {
      try {
        // get rss config from client
        Configuration filterRssConf = RssTezUtils.filterRssConf(appMaster.getConfig());
        Map<String, Edge> edges = (Map<String, Edge>) getPrivateField(dag, "edges");
        for (Map.Entry<String, Edge> entry : edges.entrySet()) {
          Edge edge = entry.getValue();

          // add user defined config to edge source conf
          Configuration edgeSourceConf =
              TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
          edgeSourceConf.set(RSS_AM_SHUFFLE_MANAGER_ADDRESS,
              this.appMaster.getTezRemoteShuffleManager().getAddress().getHostName());
          edgeSourceConf.setInt(RSS_AM_SHUFFLE_MANAGER_PORT,
              this.appMaster.getTezRemoteShuffleManager().getAddress().getPort());
          edgeSourceConf.addResource(filterRssConf);
          RssTezUtils.applyDynamicClientConf(edgeSourceConf, this.appMaster.getClusterClientConf());
          edge.getEdgeProperty().getEdgeSource()
              .setUserPayload(TezUtils.createUserPayloadFromConf(edgeSourceConf));

          // rename output class name
          OutputDescriptor outputDescriptor = edge.getEdgeProperty().getEdgeSource();
          Field outputClassNameField = outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
          outputClassNameField.setAccessible(true);
          String outputClassName = (String) outputClassNameField.get(outputDescriptor);
          String rssOutputClassName = RssTezUtils.replaceRssOutputClassName(outputClassName);
          outputClassNameField.set(outputDescriptor, rssOutputClassName);

          // add user defined config to edge destination conf
          Configuration edgeDestinationConf =
              TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
          edgeDestinationConf.set(RSS_AM_SHUFFLE_MANAGER_ADDRESS,
              this.appMaster.getTezRemoteShuffleManager().getAddress().getHostName());
          edgeDestinationConf.setInt(RSS_AM_SHUFFLE_MANAGER_PORT,
              this.appMaster.getTezRemoteShuffleManager().getAddress().getPort());
          edgeDestinationConf.addResource(filterRssConf);
          RssTezUtils.applyDynamicClientConf(edgeDestinationConf, this.appMaster.getClusterClientConf());
          edge.getEdgeProperty().getEdgeDestination()
              .setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));

          // rename input class name
          InputDescriptor inputDescriptor = edge.getEdgeProperty().getEdgeDestination();
          Field inputClassNameField = outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
          inputClassNameField.setAccessible(true);
          String inputClassName = (String) outputClassNameField.get(inputDescriptor);
          String rssInputClassName = RssTezUtils.replaceRssInputClassName(inputClassName);
          outputClassNameField.set(inputDescriptor, rssInputClassName);
        }
      } catch (IOException | IllegalAccessException | NoSuchFieldException e) {
        LOG.error("Reconfigure failed after dag was inited, caused by {}", e);
        throw new TezUncheckedException(e);
      }
    }
  }

  private static Object getPrivateField(Object object, String name) {
    try {
      Field f = object.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(object);
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  private static void reconfigureLog4j() {
    String configuratorClassName = OptionConverter.getSystemProperty(CONFIGURATOR_CLASS_KEY, null);
    String configurationOptionStr = OptionConverter.getSystemProperty(DEFAULT_CONFIGURATION_KEY, null);
    URL url = Loader.getResource(configurationOptionStr);
    OptionConverter.selectAndConfigure(url, configuratorClassName, LogManager.getLoggerRepository());
  }
}
