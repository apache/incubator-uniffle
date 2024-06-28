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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;

public class DynamicClientConfService implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicClientConfService.class);

  private Path confStoredPath;
  private FileSystem fileSystem;

  private ClientConfParser[] parsers;

  private final AtomicLong latestModificationMS = new AtomicLong(0L);
  private ScheduledExecutorService updateClientConfExecutor = null;

  private final Object clientConfLock = new Object();
  private ClientConf clientConf = null;

  private Consumer<ClientConf>[] callbacks;

  public DynamicClientConfService(CoordinatorConf coordinatorConf, Configuration hadoopConf)
      throws Exception {
    this(coordinatorConf, hadoopConf, new Consumer[0]);
  }

  public DynamicClientConfService(
      CoordinatorConf coordinatorConf, Configuration hadoopConf, Consumer<ClientConf>[] callbacks)
      throws Exception {
    if (!coordinatorConf.getBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED)) {
      return;
    }

    this.callbacks = callbacks;

    String clientConfStoredRawPath =
        coordinatorConf.getString(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH);
    this.confStoredPath = new Path(clientConfStoredRawPath);
    this.fileSystem = HadoopFilesystemProvider.getFilesystem(confStoredPath, hadoopConf);

    ClientConfParser.Parser parserType =
        coordinatorConf.get(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_RAW_CONF_PARSER);
    ClientConfParser[] confParsers;
    switch (parserType) {
      case YAML:
        confParsers = new ClientConfParser[] {new YamlClientConfParser()};
        break;
      case LEGACY:
        confParsers = new ClientConfParser[] {new LegacyClientConfParser()};
        break;
      case MIXED:
      default:
        confParsers =
            new ClientConfParser[] {new LegacyClientConfParser(), new YamlClientConfParser()};
        break;
    }
    this.parsers = confParsers;

    if (!fileSystem.isFile(confStoredPath)) {
      String msg = String.format("Fail to init, %s is not a file.", confStoredPath.toUri());
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
    refreshClientConf();
    LOGGER.info("Load client conf from {} successfully", confStoredPath);

    int updateIntervalSec =
        coordinatorConf.getInteger(
            CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC);
    updateClientConfExecutor =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor(this.getClass().getSimpleName());
    updateClientConfExecutor.scheduleAtFixedRate(
        this::refreshClientConf, 0, updateIntervalSec, TimeUnit.SECONDS);
  }

  private void refreshClientConf() {
    try {
      FileStatus[] fileStatus = fileSystem.listStatus(confStoredPath);
      if (ArrayUtils.isNotEmpty(fileStatus)) {
        long modifiedMS = fileStatus[0].getModificationTime();
        if (latestModificationMS.get() != modifiedMS) {
          doRefreshClientConf();
          latestModificationMS.set(modifiedMS);
          Arrays.stream(callbacks).forEach(x -> x.accept(clientConf));
          LOGGER.info("Update client conf from {} successfully.", confStoredPath);
        }
      } else {
        LOGGER.warn("Client conf file not found with {}", confStoredPath);
      }
    } catch (Exception e) {
      LOGGER.warn("Error when update client conf with {}.", confStoredPath, e);
    }
  }

  private void doRefreshClientConf() throws Exception {
    try (FSDataInputStream in = fileSystem.open(confStoredPath)) {
      for (ClientConfParser parser : parsers) {
        try {
          ClientConf conf = parser.tryParse(in);
          synchronized (clientConfLock) {
            this.clientConf = conf;
          }
          return;
        } catch (Exception e) {
          // ignore
        }
      }
    } catch (IOException e) {
      LOGGER.error("Fail to refresh client conf from {}", confStoredPath.toUri().toString());
      return;
    }

    throw new Exception("Unknown format of clientConf file.");
  }

  public Map<String, String> getRssClientConf() {
    synchronized (clientConfLock) {
      return clientConf.getRssClientConf();
    }
  }

  public Map<String, RemoteStorageInfo> listRemoteStorageInfos() {
    synchronized (clientConfLock) {
      return clientConf.getRemoteStorageInfos();
    }
  }

  @Override
  public void close() throws IOException {
    if (updateClientConfExecutor != null) {
      updateClientConfExecutor.shutdownNow();
    }
  }
}
