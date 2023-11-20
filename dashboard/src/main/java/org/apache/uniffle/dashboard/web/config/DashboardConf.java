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

package org.apache.uniffle.dashboard.web.config;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;

public class DashboardConf extends RssBaseConf {

  public static final ConfigOption<Integer> DASHBOARD_HTTP_PORT =
      ConfigOptions.key("rss.dashboard.http.port")
          .intType()
          .defaultValue(19988)
          .withDescription("http server http port");

  public static final ConfigOption<String> COORDINATOR_WEB_ADDRESS =
      ConfigOptions.key("coordinator.web.address")
          .stringType()
          .noDefaultValue()
          .withDescription("Coordinator jetty port request address");

  public static final ConfigOption<Long> DASHBOARD_STOP_TIMEOUT =
      ConfigOptions.key("rss.dashboard.stop.timeout")
          .longType()
          .defaultValue(30 * 1000L)
          .withDescription("dashboard http server stop timeout (ms) ");

  public static final ConfigOption<Long> DASHBOARD_IDLE_TIMEOUT =
      ConfigOptions.key("rss.dashboard.http.idle.timeout")
          .longType()
          .defaultValue(30 * 1000L)
          .withDescription("dashboard http server http idle timeout (ms) ");

  public static final ConfigOption<Integer> DASHBOARD_CORE_POOL_SIZE =
      ConfigOptions.key("rss.dashboard.corePool.size")
          .intType()
          .defaultValue(256)
          .withDescription("dashboard http server corePool size");

  public static final ConfigOption<Integer> DASHBOARD_MAX_POOL_SIZE =
      ConfigOptions.key("rss.dashboard.maxPool.size")
          .intType()
          .defaultValue(256)
          .withDescription("dashboard http server max pool size");

  public DashboardConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public boolean loadConfFromFile(String fileName) {
    return loadConfFromFile(fileName, ConfigUtils.getAllConfigOptions(DashboardConf.class));
  }
}
