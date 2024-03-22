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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.Loader;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezClassLoader;
import org.apache.tez.common.TezClientConf;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.log4j.LogManager.CONFIGURATOR_CLASS_KEY;
import static org.apache.log4j.LogManager.DEFAULT_CONFIGURATION_KEY;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT;

public class RssDAGAppMaster extends DAGAppMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RssDAGAppMaster.class);

  // RSS_SHUTDOWN_HOOK_PRIORITY is higher than SHUTDOWN_HOOK_PRIORITY(30) and will execute rss
  // shutdown hook first.
  public static final int RSS_SHUTDOWN_HOOK_PRIORITY = 50;

  private ShuffleWriteClient shuffleWriteClient;
  private TezRemoteShuffleManager tezRemoteShuffleManager;
  private Map<String, String> clusterClientConf;

  final ScheduledExecutorService heartBeatExecutorService =
      Executors.newSingleThreadScheduledExecutor(ThreadUtils.getThreadFactory("AppHeartbeat"));

  public RssDAGAppMaster(
      ApplicationAttemptId applicationAttemptId,
      ContainerId containerId,
      String nmHost,
      int nmPort,
      int nmHttpPort,
      Clock clock,
      long appSubmitTime,
      boolean isSession,
      String workingDirectory,
      String[] localDirs,
      String[] logDirs,
      String clientVersion,
      Credentials credentials,
      String jobUserName,
      AMPluginDescriptorProto pluginDescriptorProto) {
    super(
        applicationAttemptId,
        containerId,
        nmHost,
        nmPort,
        nmHttpPort,
        clock,
        appSubmitTime,
        isSession,
        workingDirectory,
        localDirs,
        logDirs,
        clientVersion,
        credentials,
        jobUserName,
        pluginDescriptorProto);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    TezClientConf rssTezConfig = new TezClientConf(conf);
    if (rssTezConfig.getBoolean(TezClientConf.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK)) {
      overrideTaskAttemptEventDispatcher();
    }
    initAndStartRSSClient(this, rssTezConfig);
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
   *
   * @param appMaster
   * @param conf
   * @throws Exception
   */
  public static void initAndStartRSSClient(final RssDAGAppMaster appMaster, TezClientConf conf)
      throws Exception {
    ShuffleWriteClient client = appMaster.getShuffleWriteClient();
    if (client == null) {
      client = RssTezUtils.createShuffleClient(conf);
      appMaster.setShuffleWriteClient(client);
    }

    String coordinators = conf.get(TezClientConf.RSS_COORDINATOR_QUORUM);
    LOG.info("Registering coordinators {}", coordinators);
    appMaster.getShuffleWriteClient().registerCoordinators(coordinators);

    String strAppAttemptId = appMaster.getAttemptID().toString();
    long heartbeatInterval = conf.getLong(TezClientConf.RSS_HEARTBEAT_INTERVAL);
    long heartbeatTimeout = conf.getLong(TezClientConf.RSS_HEARTBEAT_TIMEOUT);
    appMaster
        .getShuffleWriteClient()
        .registerApplicationInfo(strAppAttemptId, heartbeatTimeout, "user");

    appMaster.heartBeatExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            appMaster.getShuffleWriteClient().sendAppHeartbeat(strAppAttemptId, heartbeatTimeout);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Finish send heartbeat to coordinator and servers");
            }
          } catch (Exception e) {
            LOG.warn("Fail to send heartbeat to coordinator and servers", e);
          }
        },
        heartbeatInterval / 2,
        heartbeatInterval,
        TimeUnit.MILLISECONDS);

    // apply dynamic configuration
    boolean dynamicConfEnabled = conf.getBoolean(TezClientConf.RSS_DYNAMIC_CLIENT_CONF_ENABLED);
    if (dynamicConfEnabled) {
      appMaster.clusterClientConf =
          client.fetchClientConf(conf.getInteger(TezClientConf.RSS_ACCESS_TIMEOUT_MS));
    }

    RssTezUtils.applyDynamicClientConf(conf, appMaster.getClusterClientConf());

    // get remote storage from coordinator if necessary
    RemoteStorageInfo defaultRemoteStorage =
        new RemoteStorageInfo(
            conf.get(TezClientConf.RSS_REMOTE_STORAGE_PATH),
            conf.get(TezClientConf.RSS_REMOTE_STORAGE_CONF));

    String storageType = conf.get(TezClientConf.RSS_STORAGE_TYPE);
    boolean testMode = conf.get(TezClientConf.TEZ_RSS_TEST_MODE_ENABLE);
    ClientUtils.validateTestModeConf(testMode, storageType);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            appMaster.getAppID().toString(),
            defaultRemoteStorage,
            dynamicConfEnabled,
            storageType,
            client);
    // set the remote storage with actual value
    appMaster
        .getClusterClientConf()
        .put(TezClientConf.RSS_REMOTE_STORAGE_PATH.key(), remoteStorage.getPath());
    appMaster
        .getClusterClientConf()
        .put(TezClientConf.RSS_REMOTE_STORAGE_CONF.key(), remoteStorage.getConfString());

    Token<JobTokenIdentifier> sessionToken =
        TokenCache.getSessionToken(appMaster.getContext().getAppCredentials());
    appMaster.setTezRemoteShuffleManager(
        new TezRemoteShuffleManager(
            appMaster.getAppID().toString(),
            sessionToken,
            conf,
            strAppAttemptId,
            client,
            remoteStorage));
    appMaster.getTezRemoteShuffleManager().initialize();
    appMaster.getTezRemoteShuffleManager().start();

    mayCloseTezSlowStart(conf.getHadoopConfig());
  }

  @Override
  protected DAG createDAG(DAGProtos.DAGPlan dagPB) {
    DAGImpl dag = createDAG(dagPB, null);
    registerStateEnteredCallback(dag, this);
    return dag;
  }

  @Override
  public void serviceStop() throws Exception {
    releaseRssResources(this);
    super.serviceStop();
  }

  static class RssDAGAppMasterShutdownHook implements Runnable {
    RssDAGAppMaster appMaster;

    RssDAGAppMasterShutdownHook(RssDAGAppMaster appMaster) {
      this.appMaster = appMaster;
    }

    @Override
    public void run() {
      LOG.info(
          "RssDAGAppMaster received a signal. Signaling RMCommunicator and JobHistoryEventHandler.");
      this.appMaster.stop();
    }
  }

  static void releaseRssResources(RssDAGAppMaster appMaster) {
    try {
      LOG.info("RssDAGAppMaster releaseRssResources invoked");
      appMaster.heartBeatExecutorService.shutdownNow();
      if (appMaster.tezRemoteShuffleManager != null) {
        appMaster.tezRemoteShuffleManager.shutdown();
        appMaster.tezRemoteShuffleManager = null;
      }
      if (appMaster.shuffleWriteClient != null) {
        appMaster.shuffleWriteClient.close();
        appMaster.shuffleWriteClient = null;
      }
    } catch (Throwable t) {
      LOG.error("Failed to release Rss resources.", t);
    }
  }

  /**
   * main method
   *
   * @param args
   */
  public static void main(String[] args) {
    try {
      // We use trick way to introduce RssDAGAppMaster by the config tez.am.launch.cmd-opts.
      // It means some property which is set by command line will be ingored, so we must reload it.
      Configuration conf = new Configuration(new YarnConfiguration());
      DAGProtos.ConfigurationProto confProto =
          TezUtilsInternal.readUserSpecifiedTezConfiguration(
              System.getenv(ApplicationConstants.Environment.PWD.name()));
      TezUtilsInternal.addUserSpecifiedTezConfiguration(conf, confProto.getConfKeyValuesList());

      boolean sessionModeCliOption = false;
      boolean rollBackToLocalShuffle = false;
      String[] rollBackRemainingArgs = null;
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
        if (args[i].contains(DAGAppMaster.class.getName()) && isLocalShuffleMode(conf)) {
          rollBackToLocalShuffle = true;
          rollBackRemainingArgs = Arrays.copyOfRange(args, i + 1, args.length);
        }
      }

      // Load the log4j config is only init in static code block of LogManager, so we must
      // reconfigure.
      reconfigureLog4j();
      // if set tez.shuffle.mode = local then degenerates to the native way.
      if (rollBackToLocalShuffle) {
        // rollback to local shuffle mode.
        LOG.info(
            "Rollback to local shuffle mode, since tez.shuffle.mode = {}",
            conf.get(
                TezClientConf.RSS_SHUFFLE_MODE.key(),
                TezClientConf.RSS_SHUFFLE_MODE.defaultValue()));
        DAGAppMaster.main(rollBackRemainingArgs);
        return;
      }

      // Install the tez class loader, which can be used add new resources
      TezClassLoader.setupTezClassLoader();
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      final String pid = System.getenv().get("JVM_PID");
      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String appSubmitTimeStr = System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      Objects.requireNonNull(
          appSubmitTimeStr, ApplicationConstants.APP_SUBMIT_TIME_ENV + " is null");

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());

      LOG.info(
          "Creating RssDAGAppMaster for "
              + "applicationId="
              + applicationAttemptId.getApplicationId()
              + ", attemptNum="
              + applicationAttemptId.getAttemptId()
              + ", AMContainerId="
              + containerId
              + ", jvmPid="
              + pid
              + ", userFromEnv="
              + jobUserName
              + ", cliSessionOption="
              + sessionModeCliOption
              + ", pwd="
              + System.getenv(ApplicationConstants.Environment.PWD.name())
              + ", localDirs="
              + System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())
              + ", logDirs="
              + System.getenv(ApplicationConstants.Environment.LOG_DIRS.name()));

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
          new RssDAGAppMaster(
              applicationAttemptId,
              containerId,
              nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString),
              new SystemClock(),
              appSubmitTime,
              sessionModeCliOption,
              System.getenv(ApplicationConstants.Environment.PWD.name()),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
              clientVersion,
              credentials,
              jobUserName,
              amPluginDescriptorProto);
      ShutdownHookManager.get()
          .addShutdownHook(new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
      ShutdownHookManager.get()
          .addShutdownHook(new RssDAGAppMasterShutdownHook(appMaster), RSS_SHUTDOWN_HOOK_PRIORITY);

      // log the system properties
      if (LOG.isInfoEnabled()) {
        String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(conf);
        if (systemPropsToLog != null) {
          LOG.info(systemPropsToLog);
        }
      }

      if (conf.getBoolean(
              TezClientConf.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK.key(),
              TezClientConf.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK.defaultValue())
          && conf.getBoolean(
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS,
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT)) {
        LOG.info(
            "When rss.avoid.recompute.succeeded.task is enable, "
                + "we can not rescheduler succeeded task on unhealthy node");
        conf.setBoolean(TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS, false);
      }
      initAndStartAppMaster(appMaster, conf);
    } catch (Throwable t) {
      LOG.error("Error starting RssDAGAppMaster", t);
      System.exit(1);
    }
  }

  private static boolean isLocalShuffleMode(Configuration conf) {
    String shuffleMode =
        conf.get(
            TezClientConf.RSS_SHUFFLE_MODE.key(), TezClientConf.RSS_SHUFFLE_MODE.defaultValue());
    switch (shuffleMode) {
      case "remote":
        return false;
      case "local":
        return true;
      default:
        throw new RssException(
            "Unsupported shuffle mode" + shuffleMode + ", ensure that it is set to local/remote.");
    }
  }

  static void mayCloseTezSlowStart(Configuration conf) {
    if (!conf.getBoolean(
        TezClientConf.RSS_AM_SLOW_START_ENABLE.key(),
        TezClientConf.RSS_AM_SLOW_START_ENABLE.defaultValue())) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 1.0f);
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 1.0f);
    }
  }

  @VisibleForTesting
  public static void registerStateEnteredCallback(DAGImpl dag, RssDAGAppMaster appMaster) {
    StateMachineTez stateMachine = (StateMachineTez) getPrivateField(dag, "stateMachine");
    stateMachine.registerStateEnteredCallback(DAGState.INITED, new DagInitialCallback(appMaster));
    overrideDAGFinalStateCallback(
        appMaster,
        (Map) getPrivateField(stateMachine, "callbackMap"),
        Arrays.asList(DAGState.SUCCEEDED, DAGState.FAILED, DAGState.KILLED, DAGState.ERROR));
  }

  private static void overrideDAGFinalStateCallback(
      RssDAGAppMaster appMaster, Map callbackMap, List<DAGState> finalStates) {
    finalStates.forEach(
        finalState ->
            callbackMap.put(
                finalState,
                new DagFinalStateCallback(
                    appMaster, (OnStateChangedCallback) callbackMap.get(finalState))));
  }

  static class DagFinalStateCallback implements OnStateChangedCallback<DAGState, DAGImpl> {

    private RssDAGAppMaster appMaster;
    private OnStateChangedCallback callback;

    DagFinalStateCallback(RssDAGAppMaster appMaster, OnStateChangedCallback callback) {
      this.appMaster = appMaster;
      this.callback = callback;
    }

    @Override
    public void onStateChanged(DAGImpl dag, DAGState dagState) {
      callback.onStateChanged(dag, dagState);
      LOG.info("Receive a dag state change event, dagId={}, dagState={}", dag.getID(), dagState);
      long startTime = System.currentTimeMillis();
      // Generally, one application will execute multiple DAGs, and there is no correlation between
      // the DAGs.
      // Therefore, after executing a DAG, you can unregister the relevant shuffle data.
      appMaster.getTezRemoteShuffleManager().unregisterShuffleByDagId(dag.getID());
      LOG.info(
          "Complete the task of unregister shuffle, dagId={}, cost={}ms ",
          dag.getID(),
          System.currentTimeMillis() - startTime);
    }
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
          int sourceVertexId = dag.getVertex(edge.getSourceVertexName()).getVertexId().getId();
          int destinationVertexId =
              dag.getVertex(edge.getDestinationVertexName()).getVertexId().getId();

          // add user defined config to edge source conf
          Configuration edgeSourceConf =
              TezUtils.createConfFromUserPayload(
                  edge.getEdgeProperty().getEdgeSource().getUserPayload());
          edgeSourceConf.setInt(TezClientConf.RSS_SHUFFLE_SOURCE_VERTEX_ID.key(), sourceVertexId);
          edgeSourceConf.setInt(
              TezClientConf.RSS_SHUFFLE_DESTINATION_VERTEX_ID.key(), destinationVertexId);
          edgeSourceConf.set(
              TezClientConf.RSS_AM_SHUFFLE_MANAGER_ADDRESS.key(),
              this.appMaster.getTezRemoteShuffleManager().getAddress().getHostName());
          edgeSourceConf.setInt(
              TezClientConf.RSS_AM_SHUFFLE_MANAGER_PORT.key(),
              this.appMaster.getTezRemoteShuffleManager().getAddress().getPort());
          edgeSourceConf.addResource(filterRssConf);
          RssTezUtils.applyDynamicClientConf(edgeSourceConf, this.appMaster.getClusterClientConf());
          edge.getEdgeProperty()
              .getEdgeSource()
              .setUserPayload(TezUtils.createUserPayloadFromConf(edgeSourceConf));

          // rename output class name
          OutputDescriptor outputDescriptor = edge.getEdgeProperty().getEdgeSource();
          Field outputClassNameField =
              outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
          outputClassNameField.setAccessible(true);
          String outputClassName = (String) outputClassNameField.get(outputDescriptor);
          String rssOutputClassName = RssTezUtils.replaceRssOutputClassName(outputClassName);
          outputClassNameField.set(outputDescriptor, rssOutputClassName);

          // add user defined config to edge destination conf
          Configuration edgeDestinationConf =
              TezUtils.createConfFromUserPayload(
                  edge.getEdgeProperty().getEdgeSource().getUserPayload());
          edgeDestinationConf.setInt(
              TezClientConf.RSS_SHUFFLE_SOURCE_VERTEX_ID.key(), sourceVertexId);
          edgeDestinationConf.setInt(
              TezClientConf.RSS_SHUFFLE_DESTINATION_VERTEX_ID.key(), destinationVertexId);
          edgeDestinationConf.set(
              TezClientConf.RSS_AM_SHUFFLE_MANAGER_ADDRESS.key(),
              this.appMaster.getTezRemoteShuffleManager().getAddress().getHostName());
          edgeDestinationConf.setInt(
              TezClientConf.RSS_AM_SHUFFLE_MANAGER_PORT.key(),
              this.appMaster.getTezRemoteShuffleManager().getAddress().getPort());
          edgeDestinationConf.addResource(filterRssConf);
          RssTezUtils.applyDynamicClientConf(
              edgeDestinationConf, this.appMaster.getClusterClientConf());
          edge.getEdgeProperty()
              .getEdgeDestination()
              .setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));

          // rename input class name
          InputDescriptor inputDescriptor = edge.getEdgeProperty().getEdgeDestination();
          Field inputClassNameField =
              outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
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

  static void reconfigureLog4j() {
    String configuratorClassName = OptionConverter.getSystemProperty(CONFIGURATOR_CLASS_KEY, null);
    String configurationOptionStr =
        OptionConverter.getSystemProperty(DEFAULT_CONFIGURATION_KEY, null);
    URL url = Loader.getResource(configurationOptionStr);
    OptionConverter.selectAndConfigure(
        url, configuratorClassName, LogManager.getLoggerRepository());
  }

  protected void overrideTaskAttemptEventDispatcher()
      throws NoSuchFieldException, IllegalAccessException {
    AsyncDispatcher dispatcher = (AsyncDispatcher) this.getDispatcher();
    Field field = dispatcher.getClass().getDeclaredField("eventHandlers");
    field.setAccessible(true);
    Map<Class<? extends Enum>, EventHandler> eventHandlers =
        (Map<Class<? extends Enum>, EventHandler>) field.get(dispatcher);
    eventHandlers.put(TaskAttemptEventType.class, new RssTaskAttemptEventDispatcher());
  }

  private class RssTaskAttemptEventDispatcher implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = getContext().getCurrentDAG();
      int eventDagIndex = event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      Task task =
          dag.getVertex(event.getTaskAttemptID().getTaskID().getVertexID())
              .getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());

      if (attempt.getState() == TaskAttemptState.SUCCEEDED
          && event.getType() == TaskAttemptEventType.TA_NODE_FAILED) {
        // Here we only handle TA_NODE_FAILED. TA_KILL_REQUEST and TA_KILLED also could trigger
        // TerminatedAfterSuccessTransition, but the reason is not about bad node.
        LOG.info(
            "We should not recompute the succeeded task attempt, though task attempt {} received event {}",
            attempt,
            event);
        return;
      }
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }
}
