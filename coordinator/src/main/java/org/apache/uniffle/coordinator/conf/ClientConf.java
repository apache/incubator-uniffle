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

import org.apache.uniffle.common.RemoteStorageInfo;

/**
 * This class is to hold the dynamic conf, which includes the rss conf for the client and the remote
 * storage hadoop configs.
 */
public class ClientConf {
  private Map<String, String> rssClientConf;

  // key:remote-path, val: storage-conf
  private Map<String, RemoteStorageInfo> remoteStorageInfos;

  public ClientConf(
      Map<String, String> rssClientConf, Map<String, RemoteStorageInfo> remoteStorageInfos) {
    this.rssClientConf = rssClientConf;
    this.remoteStorageInfos = remoteStorageInfos;
  }

  public Map<String, String> getRssClientConf() {
    return rssClientConf;
  }

  public Map<String, RemoteStorageInfo> getRemoteStorageInfos() {
    return remoteStorageInfos;
  }
}
