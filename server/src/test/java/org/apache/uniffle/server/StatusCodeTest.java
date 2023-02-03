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

package org.apache.uniffle.server;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class StatusCodeTest {

  @Test
  public void test() throws Exception {
    RssProtos.StatusCode[] protoStatusCode = RssProtos.StatusCode.values();
    for (RssProtos.StatusCode statusCode : protoStatusCode) {
      try {
        if (RssProtos.StatusCode.UNRECOGNIZED.equals(statusCode)) {
          continue;
        }
        StatusCode.valueOf(statusCode.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    StatusCode[] statusCodes = StatusCode.values();
    for (StatusCode statusCode : statusCodes) {
      try {
        RssProtos.StatusCode.valueOf(statusCode.name());
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
    for (int i = 0; i < statusCodes.length; i++) {
      assertEquals(protoStatusCode[i], statusCodes[i].toProto());
    }
  }
}
