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
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.GetShuffleServerRequest;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.ServicePluginLifecycle;
import org.apache.tez.common.ShuffleAssignmentsInfoWritable;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.security.authorize.RssTezAMPolicyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;

import static org.apache.uniffle.common.config.RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE;

public class TezRemoteShuffleManager implements ServicePluginLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(TezRemoteShuffleManager.class);

  private InetSocketAddress address;

  protected volatile Server server;
  private String tokenIdentifier;
  private Token<JobTokenIdentifier> sessionToken;
  private Configuration conf;
  private TezRemoteShuffleUmbilicalProtocolImpl tezRemoteShuffleUmbilical;
  private ShuffleWriteClient rssClient;
  private String appId;

  public TezRemoteShuffleManager(
      String tokenIdentifier,
      Token<JobTokenIdentifier> sessionToken,
      Configuration conf,
      String appId,
      ShuffleWriteClient rssClient) {
    this.tokenIdentifier = tokenIdentifier;
    this.sessionToken = sessionToken;
    this.conf = conf;
    this.appId = appId;
    this.rssClient = rssClient;
    this.tezRemoteShuffleUmbilical = new TezRemoteShuffleUmbilicalProtocolImpl();
  }

  @Override
  public void initialize() throws Exception {}

  @Override
  public void start() throws Exception {
    startRpcServer();
  }

  @Override
  public void shutdown() throws Exception {
    server.stop();
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  private class TezRemoteShuffleUmbilicalProtocolImpl implements TezRemoteShuffleUmbilicalProtocol {
    private Map<Integer, ShuffleAssignmentsInfo> shuffleIdToShuffleAssignsInfo = new HashMap<>();

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
      return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(
        String protocol, long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    @Override
    public GetShuffleServerResponse getShuffleAssignments(GetShuffleServerRequest request)
        throws IOException, TezException {

      GetShuffleServerResponse response = new GetShuffleServerResponse();
      if (request != null) {
        LOG.info("getShuffleAssignments with request = " + request);
      } else {
        LOG.error("getShuffleAssignments with request is null");
        response.setStatus(-1);
        response.setRetMsg("GetShuffleServerRequest is null");
        return response;
      }

      int shuffleId = request.getShuffleId();
      ShuffleAssignmentsInfo shuffleAssignmentsInfo;
      try {
        synchronized (TezRemoteShuffleUmbilicalProtocolImpl.class) {
          if (shuffleIdToShuffleAssignsInfo.containsKey(shuffleId)) {
            shuffleAssignmentsInfo = shuffleIdToShuffleAssignsInfo.get(shuffleId);
          } else {
            shuffleAssignmentsInfo = getShuffleWorks(request.getPartitionNum(), shuffleId);
          }

          if (shuffleAssignmentsInfo == null) {
            response.setStatus(-1);
            response.setRetMsg("shuffleAssignmentsInfo is null");
          } else {
            response.setStatus(0);
            response.setRetMsg("");
            response.setShuffleAssignmentsInfoWritable(
                new ShuffleAssignmentsInfoWritable(shuffleAssignmentsInfo));
            shuffleIdToShuffleAssignsInfo.put(shuffleId, shuffleAssignmentsInfo);
          }
        }
      } catch (Exception rssException) {
        response.setStatus(-2);
        response.setRetMsg(rssException.getMessage());
      }

      return response;
    }
  }

  private ShuffleAssignmentsInfo getShuffleWorks(int partitionNum, int shuffleId) {
    ShuffleAssignmentsInfo shuffleAssignmentsInfo;
    int requiredAssignmentShuffleServersNum =
        RssTezUtils.getRequiredShuffleServerNumber(conf, 200, partitionNum);
    // retryInterval must bigger than `rss.server.heartbeat.timeout`, or maybe it will return the
    // same result
    long retryInterval =
        conf.getLong(
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
    int retryTimes =
        conf.getInt(
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

    // Get the configured server assignment tags and it will also add default shuffle version tag.
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = conf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
    if (StringUtils.isNotEmpty(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);

    // get remote storage from coordinator if necessary
    boolean dynamicConfEnabled =
        conf.getBoolean(
            RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
            RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);
    RemoteStorageInfo defaultRemoteStorage =
        new RemoteStorageInfo(conf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH, ""));
    String storageType = conf.get(RssTezConfig.RSS_STORAGE_TYPE);
    boolean testMode = conf.getBoolean(RssTezConfig.RSS_TEST_MODE_ENABLE, false);
    ClientUtils.validateTestModeConf(testMode, storageType);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            appId, defaultRemoteStorage, dynamicConfEnabled, storageType, rssClient);

    try {
      shuffleAssignmentsInfo =
          RetryUtils.retry(
              () -> {
                ShuffleAssignmentsInfo shuffleAssignments =
                    rssClient.getShuffleAssignments(
                        appId,
                        shuffleId,
                        partitionNum,
                        1,
                        Sets.newHashSet(assignmentTags),
                        requiredAssignmentShuffleServersNum,
                        -1);

                Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
                    shuffleAssignments.getServerToPartitionRanges();

                if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
                  return null;
                }
                LOG.info("Start to register shuffle");
                long start = System.currentTimeMillis();
                serverToPartitionRanges
                    .entrySet()
                    .forEach(
                        entry ->
                            rssClient.registerShuffle(
                                entry.getKey(),
                                appId,
                                shuffleId,
                                entry.getValue(),
                                remoteStorage,
                                ShuffleDataDistributionType.NORMAL,
                                RssTezConfig.toRssConf(conf)
                                    .get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE)));
                LOG.info(
                    "Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");
                return shuffleAssignments;
              },
              retryInterval,
              retryTimes);
    } catch (Throwable throwable) {
      LOG.error("registerShuffle failed!", throwable);
      throw new RssException("registerShuffle failed!", throwable);
    }

    return shuffleAssignmentsInfo;
  }

  protected void startRpcServer() {
    try {
      String rssAmRpcBindAddress;
      Integer rssAmRpcBindPort;
      if (conf.getBoolean(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_DEBUG, false)) {
        rssAmRpcBindAddress = conf.get(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS, "0.0.0.0");
        rssAmRpcBindPort = conf.getInt(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT, 0);
      } else {
        rssAmRpcBindAddress = "0.0.0.0";
        rssAmRpcBindPort = 0;
      }

      JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, sessionToken);
      server =
          new RPC.Builder(conf)
              .setProtocol(TezRemoteShuffleUmbilicalProtocol.class)
              .setBindAddress(rssAmRpcBindAddress)
              .setPort(rssAmRpcBindPort)
              .setInstance(tezRemoteShuffleUmbilical)
              .setNumHandlers(
                  conf.getInt(
                      TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                      TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
              .setPortRangeConfig(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE)
              .setSecretManager(jobTokenSecretManager)
              .build();

      // Enable service authorization?
      if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
        refreshServiceAcls(conf, new RssTezAMPolicyProvider());
      }

      server.start();
      InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
      this.address =
          NetUtils.createSocketAddrForHost(
              serverBindAddress.getAddress().getCanonicalHostName(), serverBindAddress.getPort());
      LOG.info("Instantiated TezRemoteShuffleManager RPC at " + this.address);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  private void refreshServiceAcls(Configuration configuration, PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
}
