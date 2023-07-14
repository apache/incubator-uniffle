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

package org.apache.uniffle.common;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ServerStatusTest {

  @Test
  public void test() throws Exception {
    assertEquals(-1, ServerStatus.UNKNOWN.code());
    assertEquals(ServerStatus.fromCode(-2), ServerStatus.UNKNOWN);
    assertEquals(ServerStatus.fromCode(Integer.MAX_VALUE), ServerStatus.UNKNOWN);
    List<RssProtos.ServerStatus> protoServerStatuses =
        Arrays.stream(RssProtos.ServerStatus.values())
            .filter(s -> !RssProtos.ServerStatus.UNRECOGNIZED.equals(s))
            .collect(Collectors.toList());

    for (RssProtos.ServerStatus statusCode : protoServerStatuses) {

      try {
        ServerStatus.valueOf(statusCode.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    List<ServerStatus> serverStatuses =
        Arrays.stream(ServerStatus.values())
            .filter(s -> !ServerStatus.UNKNOWN.equals(s))
            .collect(Collectors.toList());
    for (ServerStatus serverStatus : serverStatuses) {
      try {
        RssProtos.ServerStatus.valueOf(serverStatus.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    for (int i = 0; i < serverStatuses.size(); i++) {
      assertEquals(protoServerStatuses.get(i), serverStatuses.get(i).toProto());
      assertEquals(ServerStatus.fromProto(protoServerStatuses.get(i)), serverStatuses.get(i));
    }
  }
}
