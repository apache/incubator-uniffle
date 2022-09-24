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

package org.apache.uniffle.coordinator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;

/**
 * LowestIOSampleCostSelectStorageStrategy considers that when allocating apps to different remote paths,
 * remote paths that can write and read. Therefore, it may occur that all apps are written to the same cluster.
 * At the same time, if a cluster has read and write exceptions, we will automatically avoid the cluster.
 */
public class LowestIOSampleCostSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(LowestIOSampleCostSelectStorageStrategy.class);
  /**
   * store remote path -> application count for assignment strategy
   */
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;
  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;
  private final Configuration hdfsConf;
  private final int fileSize;
  private final int readAndWriteTimes;
  private boolean remotePathIsHealthy = true;

  public LowestIOSampleCostSelectStorageStrategy(
      Map<String, RankValue> remoteStoragePathRankValue,
      Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo,
      Map<String, RemoteStorageInfo> availableRemoteStorageInfo,
      CoordinatorConf conf) {
    this.remoteStoragePathRankValue = remoteStoragePathRankValue;
    this.appIdToRemoteStorageInfo = appIdToRemoteStorageInfo;
    this.availableRemoteStorageInfo = availableRemoteStorageInfo;
    this.hdfsConf = new Configuration();
    fileSize = conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_FILE_SIZE);
    readAndWriteTimes = conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_ACCESS_TIMES);
  }

  @VisibleForTesting
  public List<Map.Entry<String, RankValue>> sortPathByRankValue(
      String path, String testPath, long startWrite, boolean isHealthy) {
    try {
      FileSystem fs = HadoopFilesystemProvider.getFilesystem(new Path(path), hdfsConf);
      fs.delete(new Path(testPath), true);
      if (isHealthy) {
        long totalTime = System.currentTimeMillis() - startWrite;
        RankValue rankValue = remoteStoragePathRankValue.get(path);
        remoteStoragePathRankValue.put(path, new RankValue(totalTime, rankValue.getAppNum().get()));
      }
    } catch (Exception e) {
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      LOG.error("Failed to sort, we will not use this remote path {}.", path, e);
    }
    List<Map.Entry<String, RankValue>> sizeList = Lists.newCopyOnWriteArrayList(
        remoteStoragePathRankValue.entrySet()).stream().filter(Objects::nonNull).sorted((x, y) -> {
          final long xReadAndWriteTime = x.getValue().getReadAndWriteTime().get();
          final long yReadAndWriteTime = y.getValue().getReadAndWriteTime().get();
          if (xReadAndWriteTime > yReadAndWriteTime) {
            return 1;
          } else if (xReadAndWriteTime < yReadAndWriteTime) {
            return -1;
          } else {
            return Integer.compare(x.getValue().getAppNum().get(), y.getValue().getAppNum().get());
          }
        }).collect(Collectors.toList());
    LOG.error("The sorted remote path list is: {}", sizeList);
    return sizeList;
  }

  @Override
  public List<Map.Entry<String, RankValue>> detectStorage(String path) {
    if (path.startsWith(ApplicationManager.REMOTE_PATH_SCHEMA.get(0))) {
      setRemotePathIsHealthy(true);
      Path remotePath = new Path(path);
      String rssTest = path + "/rssTest";
      Path testPath = new Path(rssTest);
      long startWriteTime = System.currentTimeMillis();
      try {
        FileSystem fs = HadoopFilesystemProvider.getFilesystem(remotePath, hdfsConf);
        for (int j = 0; j < readAndWriteTimes; j++) {
          byte[] data = RandomUtils.nextBytes(fileSize);
          try (FSDataOutputStream fos = fs.create(testPath)) {
            fos.write(data);
            fos.flush();
          }
          byte[] readData = new byte[fileSize];
          int readBytes;
          try (FSDataInputStream fis = fs.open(testPath)) {
            int hasReadBytes = 0;
            do {
              readBytes = fis.read(readData);
              if (hasReadBytes < fileSize) {
                for (int i = 0; i < readBytes; i++) {
                  if (data[hasReadBytes + i] != readData[i]) {
                    RankValue rankValue = remoteStoragePathRankValue.get(path);
                    remoteStoragePathRankValue.put(path,
                        new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
                    throw new RssException("The content of reading and writing is inconsistent.");
                  }
                }
              }
              hasReadBytes += readBytes;
            } while (readBytes != -1);
          }
        }
      } catch (Exception e) {
        setRemotePathIsHealthy(false);
        LOG.error("Storage read and write error, we will not use this remote path {}.", path, e);
        RankValue rankValue = remoteStoragePathRankValue.get(path);
        remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      } finally {
        return sortPathByRankValue(path, rssTest, startWriteTime, remotePathIsHealthy);
      }
    } else {
      return Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
    }
  }

  @Override
  public synchronized String pickStorage(
      List<Map.Entry<String, RankValue>> uris, String appId) {
    for (Map.Entry<String, RankValue> uri : uris) {
      String storagePath = uri.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        appIdToRemoteStorageInfo.putIfAbsent(appId, availableRemoteStorageInfo.get(storagePath));
        return storagePath;
      }
    }
    LOG.error("No RemoteStorage available, we will default to the first.");
    return uris.get(0).getKey();
  }

  public void setRemotePathIsHealthy(boolean remotePathIsHealthy) {
    this.remotePathIsHealthy = remotePathIsHealthy;
  }

  static class RankValue {
    AtomicLong readAndWriteTime;
    AtomicInteger appNum;

    RankValue(int appNum) {
      this.readAndWriteTime = new AtomicLong(0);
      this.appNum = new AtomicInteger(appNum);
    }

    RankValue(long ratioValue, int appNum) {
      this.readAndWriteTime = new AtomicLong(ratioValue);
      this.appNum = new AtomicInteger(appNum);
    }

    public AtomicLong getReadAndWriteTime() {
      return readAndWriteTime;
    }

    public AtomicInteger getAppNum() {
      return appNum;
    }

    @Override
    public String toString() {
      return "RankValue{"
          + "readAndWriteTime=" + readAndWriteTime
          + ", appNum=" + appNum
          + '}';
    }
  }
}
