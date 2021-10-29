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

import com.google.common.collect.Lists;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileServerReadHandler implements ServerReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileServerReadHandler.class);
  private String indexFileName = "";
  private String dataFileName = "";
  private List<FileSegment> indexSegments = Lists.newArrayList();
  private int readBufferSize;
  private String appId;
  private int shuffleId;
  private int partitionId;
  private long lastIndexFileSize = 0;

  public LocalFileServerReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      RssBaseConf rssBaseConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
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

  private void initIndexReadSegment() {
    try {
      int dataSize = 0;
      int segmentSize = 0;
      long offset = 0;
      indexSegments = Lists.newArrayList();
      try (LocalFileReader reader = createFileReader(indexFileName)) {
        FileBasedShuffleSegment segment = reader.readIndex();
        while (segment != null) {
          dataSize += segment.getLength();
          segmentSize += FileBasedShuffleSegment.SEGMENT_SIZE;
          if (dataSize > readBufferSize) {
            indexSegments.add(new FileSegment(indexFileName, offset, segmentSize));
            offset += segmentSize;
            dataSize = 0;
            segmentSize = 0;
          }
          segment = reader.readIndex();
        }
        if (dataSize > 0) {
          indexSegments.add(new FileSegment(indexFileName, offset, segmentSize));
        }
      }
    } catch (Exception e) {
      String msg = "Can't init index read segment for " + indexFileName;
      LOG.warn(msg);
      throw new RuntimeException(msg, e);
    }
  }

  private synchronized List<FileBasedShuffleSegment> getDataSegments(int dataSegmentIndex) {
    List<FileBasedShuffleSegment> segments = Lists.newArrayList();
    if (indexSegments.size() > dataSegmentIndex) {
      try {
        int size = 0;
        FileSegment indexSegment = indexSegments.get(dataSegmentIndex);
        try (LocalFileReader reader = createFileReader(indexFileName)) {
          reader.skip(indexSegment.getOffset());
          FileBasedShuffleSegment segment = reader.readIndex();
          while (segment != null) {
            segments.add(segment);
            size += FileBasedShuffleSegment.SEGMENT_SIZE;
            if (size >= indexSegment.getLength()) {
              break;
            } else {
              segment = reader.readIndex();
            }
          }
        }
      } catch (Exception e) {
        String msg = "Can't read index segments for " + indexFileName;
        LOG.warn(msg);
        throw new RuntimeException(msg, e);
      }
    }
    return segments;
  }

  private DataFileSegment getDataFileSegment(int segmentIndex) {
    List<FileBasedShuffleSegment> dataSegments = getDataSegments(segmentIndex);
    if (dataSegments.isEmpty()) {
      return null;
    }

    List<BufferSegment> bufferSegments = Lists.newArrayList();
    long fileOffset = dataSegments.get(0).getOffset();
    int bufferOffset = 0;
    for (FileBasedShuffleSegment segment : dataSegments) {
      bufferSegments.add(new BufferSegment(segment.getBlockId(), bufferOffset, segment.getLength(),
          segment.getUncompressLength(), segment.getCrc(), segment.getTaskAttemptId()));
      bufferOffset += segment.getLength();
    }

    return new DataFileSegment(dataFileName, fileOffset, bufferOffset, bufferSegments);
  }

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  private LocalFileReader createFileReader(String path) throws Exception {
    return new LocalFileReader(path);
  }

  // index maybe change after first read, check index file size before read and update segments if necessary
  private synchronized void updateIndexSegments() {
    try {
      File indexFile = new File(indexFileName);
      long latestSize = indexFile.length();
      if (latestSize > lastIndexFileSize) {
        long start = System.currentTimeMillis();
        initIndexReadSegment();
        LOG.debug("Init index segments for " + indexFileName + " cost "
            + (System.currentTimeMillis() - start) + " ms");
        lastIndexFileSize = latestSize;
      }
    } catch (Exception e) {
      LOG.error("Error happened when update index segments", e);
    }
  }

  @Override
  public ShuffleDataResult getShuffleData(int segmentIndex) {
    updateIndexSegments();
    byte[] readBuffer = new byte[]{};
    DataFileSegment fileSegment = getDataFileSegment(segmentIndex);
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    if (fileSegment != null) {
      try {
        long start = System.currentTimeMillis();
        try (LocalFileReader reader = createFileReader(fileSegment.getPath())) {
          readBuffer = reader.readData(fileSegment.getOffset(), fileSegment.getLength());
        }
        bufferSegments = fileSegment.getBufferSegments();
        LOG.debug("Read File segment: " + fileSegment.getPath() + ", offset["
            + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
            + "], cost:" + (System.currentTimeMillis() - start) + " ms, for appId[" + appId
            + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
      } catch (Exception e) {
        LOG.warn("Can't read data for " + fileSegment.getPath() + ", offset["
            + fileSegment.getOffset() + "], length[" + fileSegment.getLength() + "]");
      }
    }
    return new ShuffleDataResult(readBuffer, bufferSegments);
  }
}
