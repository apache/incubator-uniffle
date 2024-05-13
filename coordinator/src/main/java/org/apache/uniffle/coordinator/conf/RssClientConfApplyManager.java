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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_CLIENT_CONF_APPLY_STRATEGY;

public class RssClientConfApplyManager implements Closeable {
  private final AbstractRssClientConfApplyStrategy strategy;
  private final DynamicClientConfService dynamicClientConfService;

  public RssClientConfApplyManager(
      CoordinatorConf conf, DynamicClientConfService dynamicClientConfService) {
    this.dynamicClientConfService = dynamicClientConfService;

    String strategyCls = conf.get(COORDINATOR_CLIENT_CONF_APPLY_STRATEGY);
    this.strategy =
        RssUtils.loadExtension(
            AbstractRssClientConfApplyStrategy.class, strategyCls, dynamicClientConfService);
  }

  public Map<String, String> apply(RssClientConfFetchInfo rssClientConfFetchInfo) {
    // to be compatible with the older client version.
    if (rssClientConfFetchInfo.isEmpty()) {
      return dynamicClientConfService.getRssClientConf();
    }
    return strategy.apply(rssClientConfFetchInfo);
  }

  @VisibleForTesting
  protected AbstractRssClientConfApplyStrategy getStrategy() {
    return strategy;
  }

  @Override
  public void close() throws IOException {
    // ignore.
  }
}
