/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.coordinator;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.ConfigUtils;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.RssUtils;
import java.util.List;
import java.util.Map;

/**
 * Configuration for Coordinator Service and rss-cluster, including service port,
 * heartbeat interval and etc.
 */
public class CoordinatorConf extends RssBaseConf {

  public static final ConfigOption<String> COORDINATOR_EXCLUDE_NODES_FILE_PATH = ConfigOptions
      .key("rss.coordinator.exclude.nodes.file.path")
      .stringType()
      .noDefaultValue()
      .withDescription("The path of configuration file which have exclude nodes");
  public static final ConfigOption<Long> COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL = ConfigOptions
      .key("rss.coordinator.exclude.nodes.check.interval.ms")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Update interval for exclude nodes");
  public static final ConfigOption<Long> COORDINATOR_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.coordinator.server.heartbeat.timeout")
      .longType()
      .defaultValue(30 * 1000L)
      .withDescription("timeout if can't get heartbeat from shuffle server");
  public static final ConfigOption<String> COORDINATOR_ASSIGNMENT_STRATEGY = ConfigOptions
      .key("rss.coordinator.assignment.strategy")
      .stringType()
      .defaultValue("PARTITION_BALANCE")
      .withDescription("Strategy for assigning shuffle server to write partitions");
  public static final ConfigOption<Long> COORDINATOR_APP_EXPIRED = ConfigOptions
      .key("rss.coordinator.app.expired")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Application expired time (ms), the heartbeat interval must be less than it");
  public static final ConfigOption<Integer> COORDINATOR_SHUFFLE_NODES_MAX = ConfigOptions
      .key("rss.coordinator.shuffle.nodes.max")
      .intType()
      .defaultValue(9)
      .withDescription("The max number of shuffle server when do the assignment");

  public CoordinatorConf() {
  }

  public CoordinatorConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);

    if (properties == null) {
      return false;
    }

    loadCommonConf(properties);

    List<ConfigOption> configOptions = ConfigUtils.getAllConfigOptions(CoordinatorConf.class);
    properties.forEach((k, v) -> {
      configOptions.forEach(config -> {
        if (config.key().equalsIgnoreCase(k)) {
          set(config, ConfigUtils.convertValue(v, config.getClazz()));
        }
      });
    });
    return true;
  }
}
