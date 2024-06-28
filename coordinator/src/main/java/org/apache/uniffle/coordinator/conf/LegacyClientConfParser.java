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

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;

public class LegacyClientConfParser implements ClientConfParser {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyClientConfParser.class);
  private static final String WHITESPACE_REGEX = "\\s+";

  @Override
  public ClientConf tryParse(InputStream fileInputStream) throws Exception {
    String content = IOUtils.toString(fileInputStream, StandardCharsets.UTF_8);

    String remoteStoragePath = "";
    String remoteStorageConf = "";

    Map<String, String> rssClientConf = new HashMap<>();

    for (String item : content.split(IOUtils.LINE_SEPARATOR_UNIX)) {
      String confItem = item.trim();
      if (StringUtils.isNotEmpty(confItem)) {
        String[] confKV = confItem.split(WHITESPACE_REGEX);
        if (confKV.length == 2) {
          if (CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key().equals(confKV[0])) {
            remoteStoragePath = confKV[1];
          } else if (CoordinatorConf.COORDINATOR_REMOTE_STORAGE_CLUSTER_CONF
              .key()
              .equals(confKV[0])) {
            remoteStorageConf = confKV[1];
          } else {
            rssClientConf.put(confKV[0], confKV[1]);
          }
        }
      }
    }

    Map<String, RemoteStorageInfo> storageInfoMap =
        parseRemoteStorageInfos(remoteStoragePath, remoteStorageConf);

    return new ClientConf(rssClientConf, storageInfoMap);
  }

  private Map<String, RemoteStorageInfo> parseRemoteStorageInfos(
      String remoteStoragePath, String remoteStorageConf) {
    if (StringUtils.isNotEmpty(remoteStoragePath)) {
      LOG.info("Parsing remote storage with {} {}", remoteStoragePath, remoteStorageConf);

      Set<String> paths = Sets.newHashSet(remoteStoragePath.split(Constants.COMMA_SPLIT_CHAR));
      Map<String, Map<String, String>> confKVs =
          CoordinatorUtils.extractRemoteStorageConf(remoteStorageConf);

      Map<String, RemoteStorageInfo> remoteStorageInfoMap = new HashMap<>();
      for (String path : paths) {
        try {
          URI uri = new URI(path);
          String host = uri.getHost();
          Map<String, String> kvs = confKVs.get(host);
          remoteStorageInfoMap.put(
              path, kvs == null ? new RemoteStorageInfo(path) : new RemoteStorageInfo(path, kvs));
        } catch (URISyntaxException e) {
          LOG.warn("The remote storage path: {} is illegal. Ignore this storage", path);
        }
      }

      return remoteStorageInfoMap;
    }

    return Collections.emptyMap();
  }
}
