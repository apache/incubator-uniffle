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

package org.apache.uniffle.storage.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.hash.MurmurHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleSegment;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.handler.impl.DataFileSegment;
import org.apache.uniffle.storage.handler.impl.HadoopFileWriter;

public class ShuffleStorageUtils {

  static final String HADOOP_PATH_SEPARATOR = "/";
  static final String HADOOP_DIRNAME_SEPARATOR = "-";
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleStorageUtils.class);

  private ShuffleStorageUtils() {}

  public static String generateDataFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_DATA_FILE_SUFFIX;
  }

  public static String generateIndexFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_INDEX_FILE_SUFFIX;
  }

  public static List<DataFileSegment> mergeSegments(
      String path, List<ShuffleSegment> segments, int readBufferSize) {
    List<DataFileSegment> dataFileSegments = Lists.newArrayList();
    if (segments != null && !segments.isEmpty()) {
      if (segments.size() == 1) {
        List<ShuffleSegment> shuffleSegments = Lists.newArrayList();
        shuffleSegments.add(
            new ShuffleSegment(
                segments.get(0).getBlockId(),
                0,
                segments.get(0).getLength(),
                segments.get(0).getUncompressLength(),
                segments.get(0).getCrc(),
                segments.get(0).getTaskAttemptId()));
        dataFileSegments.add(
            new DataFileSegment(
                path, segments.get(0).getOffset(), segments.get(0).getLength(), shuffleSegments));
      } else {
        Collections.sort(segments);
        long start = -1;
        long latestPosition = -1;
        long skipThreshold = readBufferSize / 2;
        long lastPosition = Long.MAX_VALUE;
        List<ShuffleSegment> shuffleSegments = Lists.newArrayList();
        for (ShuffleSegment segment : segments) {
          // check if there has expected skip range, eg, [20, 100], [1000, 1001] and the skip range
          // is [101, 999]
          if (start > -1 && segment.getOffset() - lastPosition > skipThreshold) {
            dataFileSegments.add(
                new DataFileSegment(path, start, (int) (lastPosition - start), shuffleSegments));
            start = -1;
          }
          // previous ShuffleSegment are merged, start new merge process
          if (start == -1) {
            shuffleSegments = Lists.newArrayList();
            start = segment.getOffset();
          }
          latestPosition = segment.getOffset() + segment.getLength();
          shuffleSegments.add(
              new ShuffleSegment(
                  segment.getBlockId(),
                  segment.getOffset() - start,
                  segment.getLength(),
                  segment.getUncompressLength(),
                  segment.getCrc(),
                  segment.getTaskAttemptId()));
          if (latestPosition - start >= readBufferSize) {
            dataFileSegments.add(
                new DataFileSegment(path, start, (int) (latestPosition - start), shuffleSegments));
            start = -1;
          }
          lastPosition = latestPosition;
        }
        if (start > -1) {
          dataFileSegments.add(
              new DataFileSegment(path, start, (int) (lastPosition - start), shuffleSegments));
        }
      }
    }
    return dataFileSegments;
  }

  public static String getShuffleDataPath(String appId, int shuffleId) {
    return String.join(HADOOP_PATH_SEPARATOR, appId, String.valueOf(shuffleId));
  }

  public static String getShuffleDataPath(String appId, int shuffleId, int start, int end) {
    return String.join(
        HADOOP_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId),
        String.join(HADOOP_DIRNAME_SEPARATOR, String.valueOf(start), String.valueOf(end)));
  }

  public static String getCombineDataPath(String appId, int shuffleId) {
    return String.join(HADOOP_PATH_SEPARATOR, appId, String.valueOf(shuffleId), "combine");
  }

  public static String getFullShuffleDataFolder(String basePath, String subPath) {
    return String.join(HADOOP_PATH_SEPARATOR, basePath, subPath);
  }

  public static String getShuffleDataPathWithRange(
      String appId, int shuffleId, int partitionId, int partitionNumPerRange, int partitionNum) {
    int prNum =
        partitionNum % partitionNumPerRange == 0
            ? partitionNum / partitionNumPerRange
            : partitionNum / partitionNumPerRange + 1;
    for (int i = 0; i < prNum; i++) {
      int start = i * partitionNumPerRange;
      int end = (i + 1) * partitionNumPerRange - 1;
      if (partitionId >= start && partitionId <= end) {
        return getShuffleDataPath(appId, shuffleId, start, end);
      }
    }
    throw new RssException(
        "Can't generate ShuffleData Path for appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], partitionId["
            + partitionId
            + "], partitionNumPerRange["
            + partitionNumPerRange
            + "], partitionNum["
            + partitionNum
            + "]");
  }

  public static int[] getPartitionRange(
      int partitionId, int partitionNumPerRange, int partitionNum) {
    int[] range = null;
    int prNum = partitionId / partitionNumPerRange;
    if (partitionId < 0 || partitionId >= partitionNum) {
      LOG.warn(
          "Invalid partitionId. partitionId:{} ,partitionNumPerRange: {}, partitionNum: {}",
          partitionId,
          partitionNumPerRange,
          partitionNum);
    } else {
      range = new int[] {partitionNumPerRange * prNum, partitionNumPerRange * (prNum + 1) - 1};
    }
    return range;
  }

  public static int getStorageIndex(int max, String appId, int shuffleId, int startPartition) {
    String hash = appId + "_" + shuffleId + "_" + startPartition;
    int index = MurmurHash.getInstance().hash(hash.getBytes(StandardCharsets.UTF_8)) % max;
    if (index < 0) {
      index = -index;
    }
    return index;
  }

  public static void createDirIfNotExist(FileSystem fileSystem, String pathString)
      throws IOException {
    Path path = new Path(pathString);
    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
      }
    } catch (IOException ioe) {
      // if folder exist, ignore the exception
      if (!fileSystem.exists(path)) {
        LOG.error(
            "Can't create shuffle folder {}, {}", pathString, ExceptionUtils.getStackTrace(ioe));
        throw ioe;
      }
    }
  }

  public static long uploadFile(File file, HadoopFileWriter writer, int bufferSize)
      throws IOException {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      return writer.copy(inputStream, bufferSize);
    } catch (IOException e) {
      LOG.error("Fail to upload file {}, {}", file.getAbsolutePath(), e);
      throw new IOException(e);
    }
  }

  public static boolean containsLocalFile(String storageType) {
    return StorageType.LOCALFILE.name().equals(storageType)
        || StorageType.LOCALFILE_HDFS.name().equals(storageType)
        || StorageType.MEMORY_LOCALFILE.name().equals(storageType)
        || StorageType.MEMORY_LOCALFILE_HDFS.name().equals(storageType);
  }
}
