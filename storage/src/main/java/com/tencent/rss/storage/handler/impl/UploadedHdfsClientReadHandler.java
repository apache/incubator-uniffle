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

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

public class UploadedHdfsClientReadHandler extends HdfsClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UploadedHdfsClientReadHandler.class);

  public UploadedHdfsClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      String storageBasePath,
      Configuration hadoopConf) {
    super(appId,
        shuffleId,
        partitionId,
        indexReadLimit,
        partitionNumPerRange,
        partitionNum,
        readBufferSize,
        expectBlockIds,
        processBlockIds,
        storageBasePath,
        hadoopConf);
    }

  @Override
  protected void init(String fullShufflePath) {
    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      LOG.warn("Can't get FileSystem for {}", baseFolder);
      return;
    }
    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "Can't list index file in  " + baseFolder;

    try {
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      LOG.warn(failedGetIndexFileMsg, e);
      return;
    }

    if (indexFiles != null && indexFiles.length != 0) {
      for (FileStatus status : indexFiles) {
        LOG.info("Find index file for shuffleId[" + shuffleId + "], partitionId["
            + partitionId + "] " + status.getPath());
        String fileNamePrefix = getFileNamePrefix(status.getPath().toUri().toString());
        try {
          HdfsShuffleReadHandler handler = new UploadedStorageHdfsShuffleReadHandler(
              appId, shuffleId, partitionId, fileNamePrefix, readBufferSize,
              expectBlockIds, processBlockIds, hadoopConf);
          readHandlers.add(handler);
        } catch (Exception e) {
          LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
        }
      }
      readHandlers.sort(Comparator.comparing(HdfsShuffleReadHandler::getFilePrefix));
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    // init lazily like LocalFileClientRead
    if (readHandlers.isEmpty()) {
      String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getUploadShuffleDataPath(appId, shuffleId, partitionId));
      init(fullShufflePath);
      String combinePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getCombineDataPath(appId, shuffleId));
      init(combinePath);
    }
    return super.readShuffleData();
  }
}
