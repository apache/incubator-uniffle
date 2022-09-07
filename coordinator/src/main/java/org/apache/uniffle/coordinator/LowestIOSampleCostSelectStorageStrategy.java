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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.util.ThreadUtils;

/**
 * LowestIOSampleCostSelectStorageStrategy considers that when allocating apps to different remote paths,
 * remote paths that can write and read. Therefore, it may occur that all apps are written to the same cluster.
 * At the same time, if a cluster has read and write exceptions, we will automatically avoid the cluster.
 */
public class LowestIOSampleCostSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(LowestIOSampleCostSelectStorageStrategy.class);
  /**
   * store appId -> remote path to make sure all shuffle data of the same application
   * will be written to the same remote storage
   */
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;
  /**
   * store remote path -> application count for assignment strategy
   */
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;
  private List<Map.Entry<String, RankValue>> sizeList;
  private FileSystem fs;
  private Configuration conf;
  private final int fileSize;
  private final int readAndWriteTimes;

  public LowestIOSampleCostSelectStorageStrategy(CoordinatorConf cf) {
    conf = new Configuration();
    fileSize = cf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_IO_SAMPLE_FILE_SIZE);
    readAndWriteTimes = cf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_IO_SAMPLE_ACCESS_TIMES);
    this.appIdToRemoteStorageInfo = Maps.newConcurrentMap();
    this.remoteStoragePathRankValue = Maps.newConcurrentMap();
    this.availableRemoteStorageInfo = Maps.newHashMap();
    this.sizeList = Lists.newCopyOnWriteArrayList();
    ScheduledExecutorService readWriteRankScheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("readWriteRankScheduler-%d"));
    // should init later than the refreshRemoteStorage init
    readWriteRankScheduler.scheduleAtFixedRate(this::checkReadAndWrite, 1000,
        cf.getLong(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_IO_SAMPLE_SCHEDULE_TIME), TimeUnit.MILLISECONDS);
  }

  public void checkReadAndWrite() {
    if (remoteStoragePathRankValue.size() > 1) {
      for (String path : remoteStoragePathRankValue.keySet()) {
        Path remotePath = new Path(path);
        Path testPath = new Path(path + "/rssTest");
        long startWriteTime = System.currentTimeMillis();
        try {
          fs = HadoopFilesystemProvider.getFilesystem(remotePath, conf);
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
                      remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
                    }
                  }
                }
                hasReadBytes += readBytes;
              } while (readBytes != -1);
            }
          }
        } catch (Exception e) {
          LOG.error("Storage read and write error, we will not use this remote path {}.", path, e);
          RankValue rankValue = remoteStoragePathRankValue.get(path);
          remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
        } finally {
          sortPathByRankValue(path, testPath, startWriteTime);
        }
      }
    } else {
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
    }
  }

  @VisibleForTesting
  public void sortPathByRankValue(String path, Path testPath, long startWrite) {
    try {
      fs.delete(testPath, true);
      long totalTime = System.currentTimeMillis() - startWrite;
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(totalTime, rankValue.getAppNum().get()));
    } catch (Exception e) {
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      LOG.error("Failed to sort, we will not use this remote path {}.", path, e);
    } finally {
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream().filter(Objects::nonNull)
          .sorted(Comparator.comparingDouble(
              entry -> entry.getValue().getReadAndWriteTime().get())).collect(Collectors.toList());
    }
  }

  /**
   * the strategy of pick remote storage is based on whether the remote path can be read or written
   */
  @Override
  public RemoteStorageInfo pickRemoteStorage(String appId) {
    if (appIdToRemoteStorageInfo.containsKey(appId)) {
      return appIdToRemoteStorageInfo.get(appId);
    }

    for (Map.Entry<String, RankValue> entry : sizeList) {
      String storagePath = entry.getKey();
      if (availableRemoteStorageInfo.containsKey(storagePath)) {
        appIdToRemoteStorageInfo.putIfAbsent(appId, availableRemoteStorageInfo.get(storagePath));
        incRemoteStorageCounter(storagePath);
        break;
      }
    }
    return appIdToRemoteStorageInfo.get(appId);
  }

  @Override
  @VisibleForTesting
  public synchronized void incRemoteStorageCounter(String remoteStoragePath) {
    RankValue counter = remoteStoragePathRankValue.get(remoteStoragePath);
    if (counter != null) {
      counter.getAppNum().incrementAndGet();
    } else {
      remoteStoragePathRankValue.put(remoteStoragePath, new RankValue(1));
      // it may be happened when assignment remote storage
      // and refresh remote storage at the same time
      LOG.warn("Remote storage path lost during assignment: %s doesn't exist, "
          + "reset the rank value to 0 and app size to 1.", remoteStoragePath);
    }
  }

  @Override
  @VisibleForTesting
  public synchronized void decRemoteStorageCounter(String storagePath) {
    if (!StringUtils.isEmpty(storagePath)) {
      RankValue atomic = remoteStoragePathRankValue.get(storagePath);
      if (atomic != null) {
        double count = atomic.getAppNum().decrementAndGet();
        if (count < 0) {
          LOG.warn("Unexpected counter for remote storage: %s, which is %i, reset to 0",
              storagePath, count);
          atomic.getAppNum().set(0);
        }
      } else {
        remoteStoragePathRankValue.putIfAbsent(storagePath, new RankValue(1));
        LOG.warn("Can't find counter for remote storage: {}", storagePath);
      }

      if (remoteStoragePathRankValue.get(storagePath).getAppNum().get() == 0
          && !availableRemoteStorageInfo.containsKey(storagePath)) {
        remoteStoragePathRankValue.remove(storagePath);
      }
    }
  }

  @Override
  public synchronized void removePathFromCounter(String storagePath) {
    RankValue rankValue = remoteStoragePathRankValue.get(storagePath);
    // The time spent reading and writing cannot be used to determine whether the current path is still used by apps.
    // Therefore, determine whether the HDFS path is still used by the number of apps
    if (rankValue != null && rankValue.getAppNum().get() == 0) {
      remoteStoragePathRankValue.remove(storagePath);
    }
  }

  @Override
  public Map<String, RemoteStorageInfo> getAppIdToRemoteStorageInfo() {
    return appIdToRemoteStorageInfo;
  }

  @Override
  public Map<String, RankValue> getRemoteStoragePathRankValue() {
    return remoteStoragePathRankValue;
  }

  @Override
  public Map<String, RemoteStorageInfo> getAvailableRemoteStorageInfo() {
    return availableRemoteStorageInfo;
  }

  @VisibleForTesting
  public void setFs(FileSystem fs) {
    this.fs = fs;
  }

  @VisibleForTesting
  public void setConf(Configuration conf) {
    this.conf = conf;
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
  }
}
