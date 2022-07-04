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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class HdfsShuffleDeleteHandler implements ShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleDeleteHandler.class);

  private Configuration hadoopConf;

  public HdfsShuffleDeleteHandler(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public void delete(String[] storageBasePaths, Set<String> subPaths) {
    try {
      FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(new Path(storageBasePaths[0]), hadoopConf);
      subPaths.forEach(subPath -> {
        delete(storageBasePaths, subPath, fileSystem);
      });
    } catch (IOException e) {
      LOG.warn("Failed to delete shuffle data in HDFS, get fileSystem error: {}", e);
    }
  }

  public void delete(String[] storageBasePaths, String subPath, FileSystem fileSystem) {
    Path path = new Path(ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePaths[0], subPath));
    boolean isSuccess = false;
    int times = 0;
    int retryMax = 5;
    long start = System.currentTimeMillis();
    LOG.info("Try delete shuffle data in HDFS for subPath[" + subPath + "] with " + path);
    while (!isSuccess && times < retryMax) {
      try {
        fileSystem.delete(path, true);
        isSuccess = true;
      } catch (Exception e) {
        times++;
        LOG.warn("Can't delete shuffle data for subPath[" + subPath + "] with " + times + " times", e);
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {
          LOG.warn("Exception happened when Thread.sleep", ex);
        }
      }
    }
    if (isSuccess) {
      LOG.info("Delete shuffle data in HDFS for subPath[" + subPath + "] with " + path + " successfully in "
          + (System.currentTimeMillis() - start) + " ms");
    } else {
      LOG.info("Failed to delete shuffle data in HDFS for subPath[" + subPath + "] with " + path + " in "
          + (System.currentTimeMillis() - start) + " ms");
    }
  }
}
