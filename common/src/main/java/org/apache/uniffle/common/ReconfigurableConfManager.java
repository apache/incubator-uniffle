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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_RECONFIGURE_INTERVAL_SEC;

public class ReconfigurableConfManager<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReconfigurableConfManager.class);

  private static ReconfigurableConfManager reconfigurableConfManager;

  private RssConf rssConf;
  private ScheduledExecutorService scheduledThreadPoolExecutor;
  private List<ConfigOption<T>> updateConfOptions;

  private long latestModificationTimestamp;

  private ReconfigurableConfManager(RssConf rssConf, String rssConfFilePath, Class confCls) {
    Supplier<RssConf> confSupplier = getConfFromFile(rssConfFilePath, confCls);
    initialize(rssConf, confSupplier);
  }

  private ReconfigurableConfManager(RssConf rssConf, Supplier<RssConf> confSupplier) {
    initialize(rssConf, confSupplier);
  }

  private void initialize(RssConf rssConf, Supplier<RssConf> confSupplier) {
    this.rssConf = new RssConf(rssConf);
    if (confSupplier != null) {
      this.updateConfOptions = new ArrayList<>();
      this.scheduledThreadPoolExecutor =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("Refresh-rss-conf");
      LOGGER.info("Starting scheduled reconfigurable conf checker...");
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
  }

  private Supplier<RssConf> getConfFromFile(String rssConfFilePath, Class confCls) {
    return () -> {
      File confFile = new File(rssConfFilePath);
      if (confFile.exists() && confFile.isFile()) {
        long lastModified = confFile.lastModified();
        if (lastModified > latestModificationTimestamp) {
          latestModificationTimestamp = lastModified;
          RssBaseConf conf = new RssBaseConf();
          conf.loadConfFromFile(rssConfFilePath, ConfigUtils.getAllConfigOptions(confCls));
          return conf;
        }
      }
      LOGGER.info("Rss conf file:{} is invalid. Ignore updating.", rssConfFilePath);
      return null;
    };
  }

  private void update(RssConf latestConf) {
    if (latestConf == null) {
      return;
    }
    for (ConfigOption<T> configOption : updateConfOptions) {
      T val = latestConf.get(configOption);
      if (!Objects.equals(val, rssConf.get(configOption))) {
        LOGGER.info(
            "Update the config option: {} from {} -> {}",
            configOption.key(),
            val,
            rssConf.get(configOption));
        rssConf.set(configOption, val);
      }
    }
  }

  private RssConf getConfRef() {
    return rssConf;
  }

  private void registerInternal(ConfigOption<T> configOption) {
    this.updateConfOptions.add(configOption);
  }

  public static void init(RssConf rssConf, String rssConfFilePath) {
    ReconfigurableConfManager manager =
        new ReconfigurableConfManager(rssConf, rssConfFilePath, rssConf.getClass());
    reconfigurableConfManager = manager;
  }

  @VisibleForTesting
  protected static void initForTest(RssConf rssConf, Supplier<RssConf> confSupplier) {
    ReconfigurableConfManager manager = new ReconfigurableConfManager(rssConf, confSupplier);
    reconfigurableConfManager = manager;
  }

  public static <T> Reconfigurable<T> register(RssConf conf, ConfigOption<T> configOption) {
    if (reconfigurableConfManager == null) {
      LOGGER.warn(
          "{} is not initialized. The conf of [{}] will not be updated.",
          ReconfigurableConfManager.class.getSimpleName(),
          configOption.key());
      return new FixedReconfigurable<>(conf, configOption);
    }

    reconfigurableConfManager.registerInternal(configOption);
    Reconfigurable<T> reconfigurable =
        new Reconfigurable<T>(reconfigurableConfManager, configOption);
    return reconfigurable;
  }

  public static class FixedReconfigurable<T> extends Reconfigurable<T> {
    RssConf conf;
    ConfigOption<T> option;

    FixedReconfigurable(RssConf conf, ConfigOption<T> option) {
      this.conf = conf;
      this.option = option;
    }

    @Override
    public T get() {
      return conf.get(option);
    }

    @Override
    public long getSizeAsBytes() {
      return conf.getSizeAsBytes((ConfigOption<Long>) option);
    }
  }

  public static class Reconfigurable<T> {
    ReconfigurableConfManager reconfigurableConfManager;
    ConfigOption<T> option;

    Reconfigurable() {}

    Reconfigurable(ReconfigurableConfManager reconfigurableConfManager, ConfigOption<T> option) {
      this.reconfigurableConfManager = reconfigurableConfManager;
      this.option = option;
    }

    public T get() {
      return reconfigurableConfManager.getConfRef().get(option);
    }

    public long getSizeAsBytes() {
      return reconfigurableConfManager.getConfRef().getSizeAsBytes((ConfigOption<Long>) option);
    }
  }
}
