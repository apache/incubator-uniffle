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

package org.apache.tez.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;

public class UmbilicalUtils {
  private static final Logger LOG = LoggerFactory.getLogger(UmbilicalUtils.class);

  private UmbilicalUtils() {
  }

  /**
   *
   * @return Get Application Master host and port from config file
   */
  private static Pair<String, Integer> getAmHostPort() {
    JobConf conf = new JobConf(RssTezConfig.RSS_CONF_FILE);
    String host = conf.get(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS, "null host");
    int port = conf.getInt(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT, -1);
    LOG.info("Got RssConf am info, host is: {}, port is: {}", host, port);
    return new ImmutablePair<>(host, port);
  }

  /**
   *
   * @param applicationId Application Id of this task
   * @param conf  Configuration
   * @param taskAttemptId task Attempt Id
   * @param shuffleId   computed using dagId, up dagName, down dagName by RssTezUtils.computeShuffleId() method.
   * @return Shuffle Server Info by request Application Master
   * @throws IOException
   * @throws InterruptedException
   * @throws TezException
   */
  private static Map<Integer, List<ShuffleServerInfo>> doRequestShuffleServer(
            ApplicationId applicationId,
            Configuration conf,
            TezTaskAttemptID taskAttemptId,
            int shuffleId) throws IOException, InterruptedException, TezException {
    UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(applicationId.toString());

    Pair<String, Integer> amHostPort = getAmHostPort();
    final InetSocketAddress address = NetUtils.createSocketAddrForHost(amHostPort.getLeft(), amHostPort.getRight());
    TezRemoteShuffleUmbilicalProtocol umbilical = taskOwner
        .doAs(new PrivilegedExceptionAction<TezRemoteShuffleUmbilicalProtocol>() {
          @Override
          public TezRemoteShuffleUmbilicalProtocol run() throws Exception {
            return RPC.getProxy(TezRemoteShuffleUmbilicalProtocol.class,
                TezRemoteShuffleUmbilicalProtocol.versionID,
                address, conf);
          }
        });
    GetShuffleServerRequest request = new GetShuffleServerRequest(taskAttemptId, 200, 200, shuffleId);

    GetShuffleServerResponse response = umbilical.getShuffleAssignments(request);
    Map<Integer, List<ShuffleServerInfo>>  partitionToServers = response.getShuffleAssignmentsInfoWritable()
        .getShuffleAssignmentsInfo()
        .getPartitionToServers();
    LOG.info("RequestShuffleServer applicationId:{}, taskAttemptId:{}, host:{}, port:{}, shuffleId:{}, worker:{}",
        applicationId, taskAttemptId, amHostPort.getLeft(), amHostPort.getRight(), shuffleId, partitionToServers);
    return partitionToServers;
  }

  public static Map<Integer, List<ShuffleServerInfo>> requestShuffleServer(
          ApplicationId applicationId,
          Configuration conf,
          TezTaskAttemptID taskAttemptId,
          int shuffleId) {
    try {
      return doRequestShuffleServer(applicationId, conf, taskAttemptId,shuffleId);
    } catch (IOException | InterruptedException | TezException e) {
      LOG.error("Failed to requestShuffleServer, applicationId:{}, taskAttemptId:{}, shuffleId:{}, worker:{}",
          applicationId, taskAttemptId, shuffleId, e);
    }
    return null;
  }
}
