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

package org.apache.uniffle.coordinator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RssUtils;

/**
 * This checker limits the number of apps that different users can submit.
 */
public class AccessQuotaChecker extends AbstractAccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessQuotaChecker.class);

  private final QuotaManager quotaManager;
  private final CoordinatorConf conf;
  private static final LongAdder COUNTER = new LongAdder();
  private final String hostIp;

  public AccessQuotaChecker(AccessManager accessManager) throws Exception {
    super(accessManager);
    conf = accessManager.getCoordinatorConf();
    quotaManager = accessManager.getQuotaManager();
    hostIp = RssUtils.getHostIp();
  }

  @Override
  public AccessCheckResult check(AccessInfo accessInfo) {
    COUNTER.increment();
    final String uuid = hostIp.hashCode() + "-" + COUNTER.sum();
    final String user = accessInfo.getUser();
    // low version client user attribute is an empty string
    if (!"".equals(user)) {
      Map<String, Map<String, Long>> currentUserApps = quotaManager.getCurrentUserAndApp();
      Map<String, Long> appAndTimes = currentUserApps.computeIfAbsent(user, x -> Maps.newConcurrentMap());
      Integer defaultAppNum = quotaManager.getDefaultUserApps().getOrDefault(user,
          conf.getInteger(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_APP_NUM));
      synchronized (ApplicationManager.class) {
        int currentAppNum = appAndTimes.size();
        if (currentAppNum >= defaultAppNum) {
          String msg = "Denied by AccessQuotaChecker => "
              + "User: " + user + ", current app num is: " + currentAppNum
              + ", default app num is: " + defaultAppNum + ". We will reject this app[uuid=" + uuid + "].";
          LOG.error(msg);
          CoordinatorMetrics.counterTotalQuotaDeniedRequest.inc();
          return new AccessCheckResult(false, msg);
        }
        appAndTimes.put(uuid, System.currentTimeMillis());
      }
    }
    return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE, uuid);
  }

  @Override
  public void close() throws IOException {

  }
}
