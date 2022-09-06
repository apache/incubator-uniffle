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

package org.apache.uniffle.client.util;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;

public class RssShuffleUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static void applyDynamicClientConf(
      Set<String> mandatoryList,
      RssConf rssConf,
      Map<String, String> confItems) {

    if (rssConf == null) {
      LOGGER.warn("Rss client conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOGGER.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String remoteKey = kv.getKey();
      String remoteVal = kv.getValue();

      if (!rssConf.containsKey(remoteKey) || mandatoryList.contains(remoteKey)) {
        LOGGER.warn("Use conf dynamic conf {} = {}", remoteKey, remoteVal);
        rssConf.setString(remoteKey, remoteVal);
      }
    }
  }
}
