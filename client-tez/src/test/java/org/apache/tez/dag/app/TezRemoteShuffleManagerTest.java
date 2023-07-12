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

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.GetShuffleServerRequest;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TezRemoteShuffleManagerTest {

  @Test
  public void testTezRemoteShuffleManager() {
    try {
      Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
      partitionToServers.put(0, new ArrayList<>());
      partitionToServers.put(1, new ArrayList<>());
      partitionToServers.put(2, new ArrayList<>());
      partitionToServers.put(3, new ArrayList<>());
      partitionToServers.put(4, new ArrayList<>());

      ShuffleServerInfo work1 = new ShuffleServerInfo("host1", 9999);
      ShuffleServerInfo work2 = new ShuffleServerInfo("host2", 9999);
      ShuffleServerInfo work3 = new ShuffleServerInfo("host3", 9999);
      ShuffleServerInfo work4 = new ShuffleServerInfo("host4", 9999);

      partitionToServers.get(0).addAll(Arrays.asList(work1, work2, work3, work4));
      partitionToServers.get(1).addAll(Arrays.asList(work1, work2, work3, work4));
      partitionToServers.get(2).addAll(Arrays.asList(work1, work3));
      partitionToServers.get(3).addAll(Arrays.asList(work3, work4));
      partitionToServers.get(4).addAll(Arrays.asList(work2, work4));

      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
      PartitionRange range0 = new PartitionRange(0, 0);
      PartitionRange range1 = new PartitionRange(1, 1);
      PartitionRange range2 = new PartitionRange(2, 2);
      PartitionRange range3 = new PartitionRange(3, 3);
      PartitionRange range4 = new PartitionRange(4, 4);

      serverToPartitionRanges.put(work1, Arrays.asList(range0, range1, range2));
      serverToPartitionRanges.put(work2, Arrays.asList(range0, range1, range4));
      serverToPartitionRanges.put(work3, Arrays.asList(range0, range1, range2, range3));
      serverToPartitionRanges.put(work4, Arrays.asList(range0, range1, range3, range4));

      ShuffleAssignmentsInfo shuffleAssignmentsInfo = new ShuffleAssignmentsInfo(partitionToServers,
              serverToPartitionRanges);

      ShuffleWriteClient client = mock(ShuffleWriteClient.class);
      when(client.getShuffleAssignments(anyString(), anyInt(), anyInt(), anyInt(), anySet(), anyInt(), anyInt()))
              .thenReturn(shuffleAssignmentsInfo);

      ApplicationId appId = ApplicationId.newInstance(9999, 72);

      Configuration conf = new Configuration();
      JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appId.toString()));
      JobTokenSecretManager secretManager = new JobTokenSecretManager();
      String tokenIdentifier = appId.toString();
      Token<JobTokenIdentifier> sessionToken = new Token(identifier, secretManager);
      secretManager.addTokenForJob(tokenIdentifier, sessionToken);
      TezRemoteShuffleManager tezRemoteShuffleManager = new TezRemoteShuffleManager(appId.toString(), sessionToken,
              conf, appId.toString(), client);
      tezRemoteShuffleManager.initialize();
      tezRemoteShuffleManager.start();

      String host = tezRemoteShuffleManager.getAddress().getHostString();
      int port = tezRemoteShuffleManager.getAddress().getPort();
      final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);

      UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(tokenIdentifier);

      TezRemoteShuffleUmbilicalProtocol umbilical = taskOwner.doAs(
          new PrivilegedExceptionAction<TezRemoteShuffleUmbilicalProtocol>() {
            @Override
            public TezRemoteShuffleUmbilicalProtocol run() throws Exception {
              return RPC.getProxy(TezRemoteShuffleUmbilicalProtocol.class,
                      TezRemoteShuffleUmbilicalProtocol.versionID, address, conf);
            }
          });

      TezDAGID dagId = TezDAGID.getInstance(appId, 1);
      TezVertexID vId = TezVertexID.getInstance(dagId, 35);
      TezTaskID tId = TezTaskID.getInstance(vId, 389);
      TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, 2);

      int mapNum = 1;
      int shuffleId = 10001;
      int reduceNum = shuffleAssignmentsInfo.getPartitionToServers().size();

      String errorMessage = "failed to get Shuffle Assignments";
      GetShuffleServerRequest request = new GetShuffleServerRequest(taId, mapNum, reduceNum, shuffleId);
      GetShuffleServerResponse response = umbilical.getShuffleAssignments(request);
      assertEquals(0, response.getStatus(), errorMessage);
      assertEquals(reduceNum, response.getShuffleAssignmentsInfoWritable().getShuffleAssignmentsInfo()
              .getPartitionToServers().size());

    } catch (Exception e) {
      e.printStackTrace();
      assertEquals("test", e.getMessage());
      fail();
    }
  }

  @Test
  public void testTezRemoteShuffleManagerSecure() {
    try {
      Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
      partitionToServers.put(0, new ArrayList<>());
      partitionToServers.put(1, new ArrayList<>());
      partitionToServers.put(2, new ArrayList<>());
      partitionToServers.put(3, new ArrayList<>());
      partitionToServers.put(4, new ArrayList<>());

      ShuffleServerInfo work1 = new ShuffleServerInfo("host1", 9999);
      ShuffleServerInfo work2 = new ShuffleServerInfo("host2", 9999);
      ShuffleServerInfo work3 = new ShuffleServerInfo("host3", 9999);
      ShuffleServerInfo work4 = new ShuffleServerInfo("host4", 9999);

      partitionToServers.get(0).addAll(Arrays.asList(work1, work2, work3, work4));
      partitionToServers.get(1).addAll(Arrays.asList(work1, work2, work3, work4));
      partitionToServers.get(2).addAll(Arrays.asList(work1, work3));
      partitionToServers.get(3).addAll(Arrays.asList(work3, work4));
      partitionToServers.get(4).addAll(Arrays.asList(work2, work4));

      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
      PartitionRange range0 = new PartitionRange(0, 0);
      PartitionRange range1 = new PartitionRange(1, 1);
      PartitionRange range2 = new PartitionRange(2, 2);
      PartitionRange range3 = new PartitionRange(3, 3);
      PartitionRange range4 = new PartitionRange(4, 4);

      serverToPartitionRanges.put(work1, Arrays.asList(range0, range1, range2));
      serverToPartitionRanges.put(work2, Arrays.asList(range0, range1, range4));
      serverToPartitionRanges.put(work3, Arrays.asList(range0, range1, range2, range3));
      serverToPartitionRanges.put(work4, Arrays.asList(range0, range1, range3, range4));

      ShuffleAssignmentsInfo shuffleAssignmentsInfo = new ShuffleAssignmentsInfo(partitionToServers,
          serverToPartitionRanges);

      ShuffleWriteClient client = mock(ShuffleWriteClient.class);
      when(client.getShuffleAssignments(anyString(), anyInt(), anyInt(), anyInt(), anySet(), anyInt(), anyInt()))
          .thenReturn(shuffleAssignmentsInfo);

      ApplicationId appId = ApplicationId.newInstance(9999, 72);

      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "kerberos");
      JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appId.toString()));
      JobTokenSecretManager secretManager = new JobTokenSecretManager();
      String tokenIdentifier = appId.toString();
      Token<JobTokenIdentifier> sessionToken = new Token(identifier, secretManager);
      Credentials credentials = new Credentials();
      TokenCache.setSessionToken(sessionToken, credentials);
      secretManager.addTokenForJob(tokenIdentifier, sessionToken);
      TezRemoteShuffleManager tezRemoteShuffleManager = new TezRemoteShuffleManager(appId.toString(), sessionToken,
          conf, appId.toString(), client);
      tezRemoteShuffleManager.initialize();
      tezRemoteShuffleManager.start();

      String host = tezRemoteShuffleManager.getAddress().getHostString();
      int port = tezRemoteShuffleManager.getAddress().getPort();
      final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);

      // here we omit the procces of deliver the credentials, just use it directly
      UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(tokenIdentifier);
      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
      SecurityUtil.setTokenService(jobToken, address);
      taskOwner.addToken(jobToken);
      TezRemoteShuffleUmbilicalProtocol umbilical = taskOwner.doAs(
          new PrivilegedExceptionAction<TezRemoteShuffleUmbilicalProtocol>() {
            @Override
            public TezRemoteShuffleUmbilicalProtocol run() throws Exception {
              return RPC.getProxy(TezRemoteShuffleUmbilicalProtocol.class,
                  TezRemoteShuffleUmbilicalProtocol.versionID, address, conf);
            }
          });

      TezDAGID dagId = TezDAGID.getInstance(appId, 1);
      TezVertexID vId = TezVertexID.getInstance(dagId, 35);
      TezTaskID tId = TezTaskID.getInstance(vId, 389);
      TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, 2);

      int mapNum = 1;
      int shuffleId = 10001;
      int reduceNum = shuffleAssignmentsInfo.getPartitionToServers().size();

      String errorMessage = "failed to get Shuffle Assignments";
      GetShuffleServerRequest request = new GetShuffleServerRequest(taId, mapNum, reduceNum, shuffleId);
      GetShuffleServerResponse response = umbilical.getShuffleAssignments(request);
      assertEquals(0, response.getStatus(), errorMessage);
      assertEquals(reduceNum, response.getShuffleAssignmentsInfoWritable().getShuffleAssignmentsInfo()
          .getPartitionToServers().size());

    } catch (Exception e) {
      e.printStackTrace();
      assertEquals("test", e.getMessage());
      fail();
    }
  }
}
