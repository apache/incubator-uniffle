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

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsShuffleWriteHandler implements ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleWriteHandler.class);

  private Configuration hadoopConf;
  private String basePath;
  private String fileNamePrefix;
  private Lock writeLock = new ReentrantLock();
  private int failTimes = 0;

  public HdfsShuffleWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf) throws IOException, IllegalStateException {
    this.hadoopConf = hadoopConf;
    this.fileNamePrefix = fileNamePrefix;
    this.basePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    initialize();
  }

  private void initialize() throws IOException, IllegalStateException {
    Path path = new Path(basePath);
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
    // check if shuffle folder exist
    if (!fileSystem.exists(path)) {
      try {
        // try to create folder, it may be created by other Shuffle Server
        fileSystem.mkdirs(path);
      } catch (IOException ioe) {
        // if folder exist, ignore the exception
        if (!fileSystem.exists(path)) {
          LOG.error("Can't create shuffle folder:" + basePath, ioe);
          throw ioe;
        }
      }
    }
  }

  @Override
  public void write(
      List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    final long start = System.currentTimeMillis();
    HdfsFileWriter dataWriter = null;
    HdfsFileWriter indexWriter = null;
    writeLock.lock();
    try {
      try {
        final long ss = System.currentTimeMillis();
        // Write to HDFS will be failed with lease problem, and can't write the same file again
        // change the prefix of file name if write failed before
        String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix + "_" + failTimes);
        String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix + "_" + failTimes);
        dataWriter = createWriter(dataFileName);
        indexWriter = createWriter(indexFileName);
        for (ShufflePartitionedBlock block : shuffleBlocks) {
          long blockId = block.getBlockId();
          long crc = block.getCrc();
          long startOffset = dataWriter.nextOffset();
          dataWriter.writeData(block.getData());

          FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
              blockId, startOffset, block.getLength(), block.getUncompressLength(), crc, block.getTaskAttemptId());
          indexWriter.writeIndex(segment);
        }
        LOG.debug(
            "Write handler inside cost {} ms for {}",
            (System.currentTimeMillis() - ss),
            fileNamePrefix);
      } catch (Exception e) {
        LOG.warn("Write failed with " + shuffleBlocks.size() + " blocks for " + fileNamePrefix + "_" + failTimes, e);
        failTimes++;
        throw new RuntimeException(e);
      } finally {
        if (dataWriter != null) {
          dataWriter.close();
        }
        if (indexWriter != null) {
          indexWriter.close();
        }
      }
    } finally {
      writeLock.unlock();
    }
    LOG.debug(
        "Write handler outside write {} blocks cost {} ms for {}",
        shuffleBlocks.size(),
        (System.currentTimeMillis() - start),
        fileNamePrefix);
  }

  private HdfsFileWriter createWriter(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    HdfsFileWriter writer = new HdfsFileWriter(path, hadoopConf);
    return writer;
  }

  @VisibleForTesting
  public void setFailTimes(int failTimes) {
    this.failTimes = failTimes;
  }
}
