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

import java.util.HashMap;
import java.util.Map;

import org.apache.uniffle.proto.RssProtos;

public enum ServerStatus {
  NORMAL_STATUS(0),
  DECOMMISSIONING(1);
  private final int statusCode;

  private static Map<Integer, ServerStatus> statusMap;

  static {
    statusMap = new HashMap<>();
    for (ServerStatus status : ServerStatus.values()) {
      statusMap.put(status.serverStatus(), status);
    }
  }

  ServerStatus(int code) {
    this.statusCode = code;
  }

  public int serverStatus() {
    return statusCode;
  }

  public RssProtos.ServerStatus toProto() {
    return RssProtos.ServerStatus.forNumber(this.serverStatus());
  }

  public static ServerStatus fromProto(RssProtos.ServerStatus status) {
    return statusMap.get(status.getNumber());
  }

}
