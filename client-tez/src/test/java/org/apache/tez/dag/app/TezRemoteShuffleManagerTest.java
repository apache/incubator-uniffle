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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.GetShuffleServerRequest;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.api.ShuffleWriteClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TezRemoteShuffleManagerTest {

  @Test
  public void test() {
    try {
      ApplicationId appId = ApplicationId.newInstance(9999, 72);

      Configuration conf = new Configuration();

      String coordinators = conf.get(RssTezConfig.RSS_COORDINATOR_QUORUM, "localhost:19999");
      ShuffleWriteClient client = RssTezUtils.createShuffleClient(conf);

      client.registerCoordinators(coordinators);

      TezRemoteShuffleManager tezRemoteShuffleManager;
      tezRemoteShuffleManager = new TezRemoteShuffleManager(appId.toString(), null, conf, appId.toString(), client);
      tezRemoteShuffleManager.initialize();
      tezRemoteShuffleManager.start();

      String host = tezRemoteShuffleManager.address.getHostString();
      int port = tezRemoteShuffleManager.address.getPort();
      final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);

      String tokenIdentifier = appId.toString();
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
      int reduceNum = 1009;

      String errorMessage = "failed to get Shuffle Assignments";
      GetShuffleServerRequest request = new GetShuffleServerRequest(taId, mapNum, reduceNum, 10001);
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
