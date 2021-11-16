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

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import java.io.FilenameFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileServerReadHandler implements ServerReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileServerReadHandler.class);
  private String indexFileName = "";
  private String dataFileName = "";
  private String appId;
  private int shuffleId;
  private int partitionId;

  public LocalFileServerReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      RssBaseConf rssBaseConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    init(appId, shuffleId, partitionId, partitionNumPerRange, partitionNum, rssBaseConf);
  }

  private void init(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      RssBaseConf rssBaseConf) {
    String allLocalPath = rssBaseConf.get(RssBaseConf.RSS_STORAGE_BASE_PATH);
    String[] storageBasePaths = allLocalPath.split(",");

    long start = System.currentTimeMillis();
    if (storageBasePaths.length > 0) {
      int[] range = ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
      int index = ShuffleStorageUtils.getStorageIndex(storageBasePaths.length, appId, shuffleId, range[0]);
      prepareFilePath(appId, shuffleId, partitionId, partitionNumPerRange, partitionNum, storageBasePaths[index]);
    } else {
      throw new RuntimeException("Can't get base path, please check rss.storage.localFile.basePaths.");
    }
    LOG.debug("Prepare for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId
        + "] cost " + (System.currentTimeMillis() - start) + " ms");
  }

  private void prepareFilePath(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      String storageBasePath) {
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(
            appId, shuffleId, partitionId, partitionNumPerRange, partitionNum));

    File baseFolder = new File(fullShufflePath);
    if (!baseFolder.exists()) {
      // the partition doesn't exist in this base folder, skip
      throw new RuntimeException("Can't find folder " + fullShufflePath);
    }
    File[] indexFiles;
    String failedGetIndexFileMsg = "No index file found in  " + storageBasePath;
    try {
      // get all index files
      indexFiles = baseFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(failedGetIndexFileMsg, e);
    }

    if (indexFiles != null && indexFiles.length > 0) {
      if (indexFiles.length != 1) {
        throw new RuntimeException("More index file than expected: " + indexFiles.length);
      }
      String fileNamePrefix = getFileNamePrefix(indexFiles[0].getName());
      indexFileName = fullShufflePath + "/" + ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
      dataFileName = fullShufflePath + "/" + ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    }
  }

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  private LocalFileReader createFileReader(String path) throws Exception {
    return new LocalFileReader(path);
  }

  @Override
  public ShuffleDataResult getShuffleData(long offset, int length) {
    byte[] readBuffer = new byte[0];

    try {
      long start = System.currentTimeMillis();
      try (LocalFileReader reader = createFileReader(dataFileName)) {
        readBuffer = reader.readData(offset, length);
      }
      LOG.debug(
          "Read File segment: {}, offset[{}], length[{}], cost: {} ms, for appId[{}], shuffleId[{}], partitionId[{}]",
          dataFileName, offset, length, System.currentTimeMillis() - start, appId, shuffleId, partitionId);
    } catch (Exception e) {
      LOG.warn("Can't read data for{}, offset[{}], length[{}]", dataFileName, offset, length);
    }

    return new ShuffleDataResult(readBuffer);
  }

  @Override
  public ShuffleIndexResult getShuffleIndex() {
    int indexNum = 0;
    int len = 0;
    try (LocalFileReader reader = createFileReader(indexFileName)) {
      indexNum = (int)  (new File(indexFileName).length() / FileBasedShuffleSegment.SEGMENT_SIZE);
      len = indexNum * FileBasedShuffleSegment.SEGMENT_SIZE;
      byte[] indexData = reader.readData(0, len);
      return new ShuffleIndexResult(indexData);
    } catch (Exception e) {
      LOG.error("Fail to read index file {} indexNum {} len {}",
          indexFileName, indexNum, len);
      return new ShuffleIndexResult();
    }
  }
}
