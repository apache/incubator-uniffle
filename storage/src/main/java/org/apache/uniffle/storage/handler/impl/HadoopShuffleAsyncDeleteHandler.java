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

package org.apache.uniffle.storage.handler.impl;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.storage.handler.AsynDeletionEvent;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.util.StorageType;

public class HadoopShuffleAsyncDeleteHandler implements ShuffleDeleteHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopShuffleAsyncDeleteHandler.class);
  private final String shuffleServerId;
  private Configuration hadoopConf;
  private AsynDeletionEventManager asynDeletionEventManager;

  public HadoopShuffleAsyncDeleteHandler(
      Configuration hadoopConf,
      String shuffleServerId,
      AsynDeletionEventManager asynDeletionEventManager) {
    this.hadoopConf = hadoopConf;
    this.shuffleServerId = shuffleServerId;
    this.asynDeletionEventManager = asynDeletionEventManager;
  }

  /** Rename the file and then delete it asynchronously. */
  @Override
  public boolean delete(String[] storageBasePaths, String appId, String user) {
    AsynDeletionEvent asynDeletionEvent =
        new AsynDeletionEvent(
            appId,
            user,
            hadoopConf,
            shuffleServerId,
            Arrays.asList(storageBasePaths),
            StorageType.HDFS.name());
    for (Map.Entry<String, String> appIdNeedDeletePaths :
        asynDeletionEvent.getNeedDeletePathAndRenamePath().entrySet()) {
      final Path path = new Path(appIdNeedDeletePaths.getKey());
      final Path breakdownPathFolder = new Path(appIdNeedDeletePaths.getValue());
      boolean isExists = false;
      boolean isSuccess = false;
      int times = 0;
      int retryMax = 5;
      long start = System.currentTimeMillis();
      LOG.info(
          "Try rename shuffle data in Hadoop FS for appId[{}] of user[{}] with {}",
          appId,
          user,
          path);
      while (!isSuccess && times < retryMax) {
        try {
          FileSystem fileSystem = HadoopFilesystemProvider.getFilesystem(user, path, hadoopConf);
          isExists = fileSystem.exists(path);
          if (isExists) {
            isSuccess = fileSystem.rename(path, breakdownPathFolder);
          } else {
            break;
          }
        } catch (Exception e) {
          if (e instanceof FileNotFoundException) {
            LOG.info("[{}] doesn't exist, ignore it.", path);
            return false;
          }
          times++;
          LOG.warn("Can't rename shuffle data for appId[{}] with {} times", appId, times, e);
          try {
            Thread.sleep(1000);
          } catch (Exception ex) {
            LOG.warn("Exception happened when Thread.sleep", ex);
          }
        }
      }
      if (isExists) {
        if (isSuccess) {
          LOG.info(
              "Rename shuffle data in Hadoop FS for appId[{}] with {} successfully in {} ms",
              appId,
              path,
              (System.currentTimeMillis() - start));
        } else {
          LOG.warn(
              "Failed to rename shuffle data in Hadoop FS for appId [{}] with {} successfully in {} ms",
              appId,
              path,
              (System.currentTimeMillis() - start));
        }
      } else {
        LOG.info(
            "Rename shuffle data in Hadoop FS for appId[{}] with {} is not exists", appId, path);
      }
    }
    if (!asynDeletionEventManager.handlerDeletionQueue(asynDeletionEvent)) {
      LOG.warn(
          "Remove the case where the twoPhasesDeletionEventQueue queue is full and cannot accept elements.");
      return false;
    }
    return true;
  }
}
