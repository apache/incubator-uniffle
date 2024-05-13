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

package org.apache.uniffle.client.request;

import java.util.Collections;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.proto.RssProtos;

public class RssFetchClientConfRequest {
  private final int timeoutMs;
  private String user;
  private Map<String, String> properties = Collections.emptyMap();

  public RssFetchClientConfRequest(int timeoutMs, String user, Map<String, String> properties) {
    this.timeoutMs = timeoutMs;
    this.user = user;
    this.properties = properties;
  }

  @VisibleForTesting
  public RssFetchClientConfRequest(int timeoutMs) {
    this.timeoutMs = timeoutMs;
    this.user = StringUtils.EMPTY;
  }

  public int getTimeoutMs() {
    return timeoutMs;
  }

  public String getUser() {
    return user;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public RssProtos.FetchClientConfRequest toProto() {
    return RssProtos.FetchClientConfRequest.newBuilder()
        .setUser(user)
        .putAllProperties(properties)
        .build();
  }
}
