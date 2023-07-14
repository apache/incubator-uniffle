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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetShuffleServerRequestTest {
  @Test
  public void testSerDe() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);

    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.getInstance(tId, 2);
    int startIndex = 1;
    int partitionNum = 20;
    int shuffleId = 1998;

    GetShuffleServerRequest request =
        new GetShuffleServerRequest(tezTaskAttemptID, startIndex, partitionNum, shuffleId);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bos);
    request.write(out);

    GetShuffleServerRequest deSerRequest = new GetShuffleServerRequest();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInput in = new DataInputStream(bis);
    deSerRequest.readFields(in);

    assertEquals(request.getCurrentTaskAttemptID(), deSerRequest.getCurrentTaskAttemptID());
    assertEquals(request.getStartIndex(), deSerRequest.getStartIndex());
    assertEquals(request.getPartitionNum(), deSerRequest.getPartitionNum());
    assertEquals(request.getShuffleId(), deSerRequest.getShuffleId());
  }
}
