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

package org.apache.uniffle.common.rpc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class StatusCodeTest {

  @Test
  public void test() throws Exception {
    assertEquals(-1, StatusCode.UNKNOWN.statusCode());
    assertEquals(StatusCode.fromCode(-2), StatusCode.UNKNOWN);
    assertEquals(StatusCode.fromCode(Integer.MAX_VALUE), StatusCode.UNKNOWN);
    List<RssProtos.StatusCode> protoStatusCode =
        Arrays.stream(RssProtos.StatusCode.values())
            .filter(s -> !RssProtos.StatusCode.UNRECOGNIZED.equals(s))
            .collect(Collectors.toList());

    for (RssProtos.StatusCode statusCode : protoStatusCode) {
      try {
        StatusCode.valueOf(statusCode.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    List<StatusCode> statusCodes =
        Arrays.stream(StatusCode.values())
            .filter(s -> !StatusCode.UNKNOWN.equals(s))
            .collect(Collectors.toList());

    for (StatusCode statusCode : statusCodes) {
      try {
        RssProtos.StatusCode.valueOf(statusCode.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    for (int i = 0; i < statusCodes.size(); i++) {
      assertEquals(protoStatusCode.get(i), statusCodes.get(i).toProto());
      assertEquals(StatusCode.fromProto(protoStatusCode.get(i)), statusCodes.get(i));
    }
  }
}
