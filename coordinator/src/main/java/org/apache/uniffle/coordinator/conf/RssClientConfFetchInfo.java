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

package org.apache.uniffle.coordinator.conf;

import java.util.Map;

import org.apache.uniffle.proto.RssProtos;

public class RssClientConfFetchInfo {
  private String user;
  private Map<String, String> properties;

  public static final RssClientConfFetchInfo EMPTY_CLIENT_CONF_FETCH_INFO =
      new RssClientConfFetchInfo(null, null);

  public RssClientConfFetchInfo(String user, Map<String, String> properties) {
    this.user = user;
    this.properties = properties;
  }

  public String getUser() {
    return user;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean isEmpty() {
    return user == null && (properties == null || properties.isEmpty());
  }

  public static RssClientConfFetchInfo fromProto(RssProtos.FetchClientConfRequest request) {
    return new RssClientConfFetchInfo(request.getUser(), request.getPropertiesMap());
  }
}
