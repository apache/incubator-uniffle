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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.common.security.JobTokenSelector;

@TokenInfo(JobTokenSelector.class)
@InterfaceAudience.Private
@InterfaceStability.Stable

// ProtocolInfo will be required once we move to Hadoop PB RPC
// @ProtocolInfo(protocolName = "TezRemoteShuffleUmbilicalProtocol", protocolVersion = 1)
public interface TezRemoteShuffleUmbilicalProtocol extends VersionedProtocol {
  @SuppressWarnings("checkstyle:ConstantName")
  long versionID = 31L;

  GetShuffleServerResponse getShuffleAssignments(GetShuffleServerRequest request)
      throws IOException, TezException;
}
