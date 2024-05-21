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

package org.apache.uniffle.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_RECONFIGURE_INTERVAL_SEC;

public class ReconfigurableConfManager<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReconfigurableConfManager.class);

  private static ReconfigurableConfManager reconfigurableConfManager;

  private RssConf rssConf;
  private ScheduledExecutorService scheduledThreadPoolExecutor;
  private List<ConfigOption<T>> updateConfOptions;

  private ReconfigurableConfManager(RssConf rssConf, Supplier<RssConf> confSupplier) {
    this.rssConf = rssConf;
    this.updateConfOptions = new ArrayList<>();
    this.scheduledThreadPoolExecutor =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("Refresh-rss-conf");
    scheduledThreadPoolExecutor.scheduleAtFixedRate(
        () -> {
          try {
            RssConf latestConf = confSupplier.get();
            update(latestConf);
          } catch (Exception e) {
            LOGGER.error("Errors on refreshing the rss conf.", e);
          }
        },
        1,
        rssConf.get(RSS_RECONFIGURE_INTERVAL_SEC),
        TimeUnit.SECONDS);
  }

  private void update(RssConf latestConf) {
    for (ConfigOption<T> configOption : updateConfOptions) {
      T val = latestConf.get(configOption);
      if (!isSame(val, rssConf.get(configOption))) {
        LOGGER.info(
            "Update the config option: {} from {} -> {}",
            configOption.key(),
            val,
            rssConf.get(configOption));
        rssConf.set(configOption, val);
      }
    }
  }

  private boolean isSame(Object v1, Object v2) {
    if (v1 == null && v2 == null) {
      return true;
    }
    if (v1 != null && v1.equals(v2)) {
      return true;
    }
    if (v2 != null && v2.equals(v1)) {
      return true;
    }
    return false;
  }

  public static void init(RssConf rssConf, Supplier<RssConf> confSupplier) {
    ReconfigurableConfManager manager = new ReconfigurableConfManager(rssConf, confSupplier);
    reconfigurableConfManager = manager;
  }

  private RssConf getConfRef() {
    return rssConf;
  }

  private void registerInternal(ConfigOption<T> configOption) {
    this.updateConfOptions.add(configOption);
  }

  public static <T> Reconfigurable<T> register(ConfigOption<T> configOption) {
    reconfigurableConfManager.registerInternal(configOption);
    Reconfigurable<T> reconfigurable =
        new Reconfigurable<T>(reconfigurableConfManager, configOption);
    return reconfigurable;
  }

  static class Reconfigurable<T> {
    ReconfigurableConfManager reconfigurableConfManager;
    ConfigOption<T> option;

    Reconfigurable(ReconfigurableConfManager reconfigurableConfManager, ConfigOption<T> option) {
      this.reconfigurableConfManager = reconfigurableConfManager;
      this.option = option;
    }

    T get() {
      return reconfigurableConfManager.getConfRef().get(option);
    }
  }
}
