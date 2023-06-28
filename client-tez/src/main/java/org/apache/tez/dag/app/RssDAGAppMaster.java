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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezClassLoader;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
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

import static org.apache.tez.common.TezCommonUtils.TEZ_SYSTEM_SUB_DIR;

public class RssDAGAppMaster extends DAGAppMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RssDAGAppMaster.class);
  private ShuffleWriteClient shuffleWriteClient;
  private TezRemoteShuffleManager tezRemoteShuffleManager;
  private static final String rssConfFileLocalResourceName = "rss_conf.xml";

  private DAGProtos.PlanLocalResource rssConfFileLocalResource;
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

  /**
   * Init and Start Rss Client
   * @param appMaster
   * @param conf
   * @param applicationAttemptId
   * @throws Exception
   */
  public static void initAndStartRSSClient(final RssDAGAppMaster appMaster, Configuration conf,
          ApplicationAttemptId applicationAttemptId) throws Exception {

    ShuffleWriteClient client = RssTezUtils.createShuffleClient(conf);
    appMaster.setShuffleWriteClient(client);

    String coordinators = conf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);

    String strAppAttemptId = applicationAttemptId.toString();
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

    TezConfiguration extraConf = new TezConfiguration(false);
    extraConf.clear();

    String strAppId = applicationAttemptId.getApplicationId().toString();
    extraConf.set(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS,
            appMaster.getTezRemoteShuffleManager().address.getHostName());
    extraConf.setInt(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT,
            appMaster.getTezRemoteShuffleManager().address.getPort());
    writeExtraConf(appMaster, conf, extraConf, strAppId);

    mayCloseTezSlowStart(conf);
  }

  @Override
  protected DAG createDAG(DAGProtos.DAGPlan dagPB) {
    DAGImpl dag = createDAG(dagPB, null);
    registerStateEnteredCallback(dag);
    return dag;
  }

  @Override
  public String submitDAGToAppMaster(DAGProtos.DAGPlan dagPlan, Map<String, LocalResource> additionalResources)
          throws TezException {

    addAdditionalResource(dagPlan, getRssConfFileLocalResource());

    return super.submitDAGToAppMaster(dagPlan, additionalResources);
  }

  public DAGProtos.PlanLocalResource getRssConfFileLocalResource() {
    return rssConfFileLocalResource;
  }

  public static void addAdditionalResource(DAGProtos.DAGPlan dagPlan, DAGProtos.PlanLocalResource additionalResource)
          throws TezException {
    List<DAGProtos.PlanLocalResource> planLocalResourceList = dagPlan.getLocalResourceList();

    if (planLocalResourceList == null) {
      LOG.warn("planLocalResourceList is null, add new list");
      planLocalResourceList = new ArrayList<>();
    } else {
      planLocalResourceList = new ArrayList<>(planLocalResourceList);
    }

    try {
      planLocalResourceList.add(additionalResource);
      Field field = DAGProtos.DAGPlan.class.getDeclaredField("localResource_");
      field.setAccessible(true);
      field.set(dagPlan, planLocalResourceList);
      field.setAccessible(false);
    } catch (Exception e) {
      LOG.error("submitDAGToAppMaster reflect error", e);
      throw new TezException(e.getMessage());
    }

    if (LOG.isDebugEnabled()) {
      for (DAGProtos.PlanLocalResource localResource : dagPlan.getLocalResourceList()) {
        LOG.debug("localResource: {}", localResource.toString());
      }
    }
  }

  /**
   * main method
   * @param args
   */
  public static void main(String[] args) {
    try {
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

      boolean sessionModeCliOption = false;
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("-D")) {
          String[] property = args[i].split("=");
          if (property.length < 2) {
            System.setProperty(property[0], "");
          } else {
            System.setProperty(property[0], property[1]);
          }
        } else if (args[i].contains("--session") || args[i].contains("-s")) {
          sessionModeCliOption = true;
        }
      }

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

      initAndStartRSSClient(appMaster, conf, applicationAttemptId);
      initAndStartAppMaster(appMaster, conf);
    } catch (Throwable t) {
      LOG.error("Error starting RssDAGAppMaster", t);
      System.exit(1);
    }
  }

  static void writeExtraConf(final RssDAGAppMaster appMaster, Configuration conf,
          TezConfiguration extraConf, String strAppId) {
    try {
      Path baseStagingPath = TezCommonUtils.getTezBaseStagingPath(conf);
      Path tezStagingDir = new Path(new Path(baseStagingPath, TEZ_SYSTEM_SUB_DIR), strAppId);

      FileSystem fs = tezStagingDir.getFileSystem(conf);
      Path rssConfFilePath = new Path(tezStagingDir, RssTezConfig.RSS_CONF_FILE);

      try (FSDataOutputStream out =
                   FileSystem.create(fs, rssConfFilePath,
                           new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
        extraConf.writeXml(out);
      }
      FileStatus rsrcStat = fs.getFileStatus(rssConfFilePath);

      appMaster.rssConfFileLocalResource = DAGProtos.PlanLocalResource.newBuilder()
              .setName(appMaster.rssConfFileLocalResourceName)
              .setUri(rsrcStat.getPath().toString())
              .setSize(rsrcStat.getLen())
              .setTimeStamp(rsrcStat.getModificationTime())
              .setType(DAGProtos.PlanLocalResourceType.FILE)
              .setVisibility(DAGProtos.PlanLocalResourceVisibility.APPLICATION)
              .build();
      LOG.info("Upload extra conf success!");
    } catch (Exception e) {
      LOG.error("Upload extra conf exception!", e);
      throw new RssException("Upload extra conf exception ", e);
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
  public static void registerStateEnteredCallback(DAGImpl dag) {
    StateMachineTez
        stateMachine = (StateMachineTez) getPrivateField(dag, "stateMachine");
    stateMachine.registerStateEnteredCallback(DAGState.INITED, new DagInitialCallback());
  }

  static class DagInitialCallback implements OnStateChangedCallback<DAGState, DAGImpl> {

    @Override
    public void onStateChanged(DAGImpl dag, DAGState dagState) {
      try {
        Map<String, Edge> edges = (Map<String, Edge>) getPrivateField(dag, "edges");
        for (Map.Entry<String, Edge> entry : edges.entrySet()) {
          Edge edge = entry.getValue();

          OutputDescriptor outputDescriptor = edge.getEdgeProperty().getEdgeSource();
          Field outputClassNameField = outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
          outputClassNameField.setAccessible(true);
          String outputClassName = (String) outputClassNameField.get(outputDescriptor);
          String rssOutputClassName = RssTezUtils.replaceRssOutputClassName(outputClassName);
          outputClassNameField.set(outputDescriptor, rssOutputClassName);

          InputDescriptor inputDescriptor = edge.getEdgeProperty().getEdgeDestination();
          Field inputClassNameField = outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
          inputClassNameField.setAccessible(true);
          String inputClassName = (String) outputClassNameField.get(inputDescriptor);
          String rssInputClassName = RssTezUtils.replaceRssInputClassName(inputClassName);
          outputClassNameField.set(inputDescriptor, rssInputClassName);
        }
      } catch (Exception e) {
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
}
