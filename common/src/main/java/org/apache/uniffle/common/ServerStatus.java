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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.uniffle.proto.RssProtos;

public enum ServerStatus {
  ACTIVE(0),
  DECOMMISSIONING(1),
  DECOMMISSIONED(2),
  LOST(3),
  UNHEALTHY(4),
  EXCLUDED(5),
  UNKNOWN(-1);

  static final Map<Integer, ServerStatus> VALUE_MAP =
      Arrays.stream(ServerStatus.values()).collect(Collectors.toMap(ServerStatus::code, s -> s));
  private final int code;

  ServerStatus(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }

  public static ServerStatus fromCode(Integer code) {
    ServerStatus serverStatus = VALUE_MAP.get(code);
    return serverStatus == null ? UNKNOWN : serverStatus;
  }

  public RssProtos.ServerStatus toProto() {
    RssProtos.ServerStatus serverStatus = RssProtos.ServerStatus.forNumber(this.code());
    return serverStatus == null ? RssProtos.ServerStatus.UNRECOGNIZED : serverStatus;
  }

  public static ServerStatus fromProto(RssProtos.ServerStatus status) {
    return fromCode(status.getNumber());
  }
}
