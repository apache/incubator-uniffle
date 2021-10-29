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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHdfsClientReadHandler extends AbstractFileClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsClientReadHandler.class);
  protected int indexReadLimit;
  protected Map<String, HdfsFileReader> dataReaderMap = Maps.newHashMap();
  protected Map<String, HdfsFileReader> indexReaderMap = Maps.newHashMap();
  protected List<FileSegment> indexSegments = Lists.newArrayList();
  protected int partitionNumPerRange;
  protected int partitionNum;
  protected int readBufferSize;
  protected String storageBasePath;
  protected AtomicLong readIndexTime = new AtomicLong(0);
  protected AtomicLong readDataTime = new AtomicLong(0);
  protected Configuration hadoopConf;

  protected void init(String fullShufflePath) {
    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get FileSystem for " + baseFolder);
    }

    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "Can't list index file in  " + baseFolder;

    try {
      // get all index files
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      LOG.error(failedGetIndexFileMsg, e);
      return;
    }

    if (indexFiles != null) {
      for (FileStatus status : indexFiles) {
        LOG.info("Find index file for shuffleId[" + shuffleId + "], partitionId["
            + partitionId + "] " + status.getPath());
        String fileNamePrefix = getFileNamePrefix(status.getPath().getName());
        try {
          dataReaderMap.put(fileNamePrefix,
              createHdfsReader(fullShufflePath, ShuffleStorageUtils.generateDataFileName(fileNamePrefix), hadoopConf));
          indexReaderMap.put(fileNamePrefix,
              createHdfsReader(fullShufflePath, ShuffleStorageUtils.generateIndexFileName(fileNamePrefix), hadoopConf));
        } catch (Exception e) {
          LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
        }
      }
    }
  }

  protected String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  protected void readAllIndexSegments() {
    for (Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "] with " + path);
        ShuffleReader reader = entry.getValue();
        long start = System.currentTimeMillis();
        List<FileBasedShuffleSegment> segments = reader.readIndex(indexReadLimit);
        int dataSize = 0;
        int segmentSize = 0;
        long offset = 0;
        while (!segments.isEmpty()) {
          for (FileBasedShuffleSegment segment : segments) {
            dataSize += segment.getLength();
            segmentSize += FileBasedShuffleSegment.SEGMENT_SIZE;
            if (dataSize > readBufferSize) {
              indexSegments.add(new FileSegment(path, offset, segmentSize));
              offset += segmentSize;
              dataSize = 0;
              segmentSize = 0;
            }
          }
          segments = reader.readIndex(indexReadLimit);
        }
        if (dataSize > 0) {
          indexSegments.add(new FileSegment(path, offset, segmentSize));
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
  }

  @Override
  public ShuffleDataResult readShuffleData(int segmentIndex) {
    ShuffleDataResult result = null;
    try {
      DataFileSegment fileSegment = getDataFileSegment(segmentIndex);
      if (fileSegment != null) {
        final long start = System.currentTimeMillis();
        byte[] readBuffer;
        List<BufferSegment> bufferSegments;
        HdfsFileReader reader = dataReaderMap.get(fileSegment.getPath());
        readBuffer = reader.readData(fileSegment.getOffset(), fileSegment.getLength());
        bufferSegments = fileSegment.getBufferSegments();
        result = new ShuffleDataResult(readBuffer, bufferSegments);
        LOG.debug("Read File segment: " + fileSegment.getPath() + ", offset["
            + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
            + "], cost:" + (System.currentTimeMillis() - start) + " ms, for appId[" + appId
            + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
      }
    } catch (Exception e) {
      LOG.warn("Can't read data for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
    }
    return result;
  }

  @Override
  public synchronized void close() {
    for (Map.Entry<String, HdfsFileReader> entry : dataReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".data";
        LOG.warn(message, ioe);
      }
    }

    for (Map.Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".index";
        LOG.warn(message, ioe);
      }
    }
    LOG.info("HdfsClientReadHandler for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId["
        + partitionId + "] cost " + readIndexTime.get() + " ms for read index and "
        + readDataTime.get() + " ms for read data");
  }

  protected HdfsFileReader createHdfsReader(
      String folder, String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(folder, fileName);
    HdfsFileReader reader = new HdfsFileReader(path, hadoopConf);
    return reader;
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

    return new DataFileSegment(indexSegments.get(segmentIndex).getPath(),
        fileOffset, bufferOffset, bufferSegments);
  }

  protected List<FileBasedShuffleSegment> getDataSegments(int dataSegmentIndex) {
    List<FileBasedShuffleSegment> segments = Lists.newArrayList();
    String path = "";
    if (indexSegments.size() > dataSegmentIndex) {
      try {
        int size = 0;
        FileSegment indexSegment = indexSegments.get(dataSegmentIndex);
        path = indexSegment.getPath();
        HdfsFileReader reader = indexReaderMap.get(path);
        reader.seek(indexSegment.getOffset());
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
      } catch (Exception e) {
        String msg = "Can't read index segments for " + path;
        LOG.warn(msg);
        throw new RuntimeException(msg, e);
      }
    }
    return segments;
  }
}
