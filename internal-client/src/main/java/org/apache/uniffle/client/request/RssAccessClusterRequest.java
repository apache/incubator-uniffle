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

import java.util.Set;

public class RssAccessClusterRequest {

  private final String accessId;
  private final Set<String> tags;
  private final int timeoutMs;

  public RssAccessClusterRequest(String accessId, Set<String> tags, int timeoutMs) {
    this.accessId = accessId;
    this.tags = tags;
    this.timeoutMs = timeoutMs;
  }

  public String getAccessId() {
    return accessId;
  }

  public Set<String> getTags() {
    return tags;
  }

  public int getTimeoutMs() {
    return timeoutMs;
  }
}
