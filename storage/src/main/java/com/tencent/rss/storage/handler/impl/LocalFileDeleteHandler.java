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

package com.tencent.rss.storage.handler.impl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

public class LocalFileDeleteHandler implements ShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileDeleteHandler.class);

  @Override
  public void delete(String[] storageBasePaths, String appId) {
    for (String basePath : storageBasePaths) {
      String shufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(basePath, appId);
      long start = System.currentTimeMillis();
      try {
        File baseFolder = new File(shufflePath);
        FileUtils.deleteDirectory(baseFolder);
        LOG.info("Delete shuffle data for appId[" + appId + "] with " + shufflePath
            + " cost " + (System.currentTimeMillis() - start) + " ms");
      } catch (Exception e) {
        LOG.warn("Can't delete shuffle data for appId[" + appId + "] with " + shufflePath, e);
      }
    }
  }
}
