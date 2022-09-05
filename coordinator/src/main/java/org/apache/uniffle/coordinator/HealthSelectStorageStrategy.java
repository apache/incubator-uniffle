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
 * HealthSelectStorageStrategy considers that when allocating apps to different remote paths,
 * remote paths that can write data faster should be allocated as much as possible.
 * Therefore, it may occur that all apps are written to the same cluster.
 * At the same time, if a cluster has read and write exceptions, we will automatically avoid the cluster.
 * If there is an exception when getting the filesystem at the beginning,
 * roll back to using AppBalanceSelectStorageStrategy.
 */
public class HealthSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(HealthSelectStorageStrategy.class);
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

  public HealthSelectStorageStrategy(CoordinatorConf cf) {
    conf = new Configuration();
    fileSize = cf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_FILE_SIZE);
    this.appIdToRemoteStorageInfo = Maps.newConcurrentMap();
    this.remoteStoragePathRankValue = Maps.newConcurrentMap();
    this.availableRemoteStorageInfo = Maps.newHashMap();
    this.sizeList = Lists.newCopyOnWriteArrayList();
    ScheduledExecutorService readWriteRankScheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("readWriteRankScheduler-%d"));
    // should init later than the refreshRemoteStorage init
    readWriteRankScheduler.scheduleAtFixedRate(this::checkReadAndWrite, 1000,
        cf.getLong(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_TIME), TimeUnit.MILLISECONDS);
  }

  public void checkReadAndWrite() {
    try {
      if (remoteStoragePathRankValue.size() > 1) {
        for (String path : remoteStoragePathRankValue.keySet()) {
          Path remotePath = new Path(path);
          fs = HadoopFilesystemProvider.getFilesystem(remotePath, conf);
          Path testPath = new Path(path + "/rssTest");
          long startWriteTime = System.currentTimeMillis();
          try {
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
          } catch (Exception e) {
            LOG.error("Storage read and write error ", e);
            RankValue rankValue = remoteStoragePathRankValue.get(path);
            remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
          } finally {
            sortPathByIORank(path, testPath, startWriteTime);
          }
        }
      } else {
        sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
      }
    } catch (Exception e) {
      LOG.error("Some error happened, compare the number of apps to select a remote path", e);
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream().filter(Objects::nonNull)
          .sorted(Comparator.comparingDouble(entry -> entry.getValue().getAppNum().get())).collect(Collectors.toList());
    }
  }

  @VisibleForTesting
  public void sortPathByIORank(String path, Path testPath, long startWrite) {
    try {
      fs.deleteOnExit(testPath);
      long totalTime = System.currentTimeMillis() - startWrite;
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(totalTime, rankValue.getAppNum().get()));
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream().filter(Objects::nonNull)
          .sorted(Comparator.comparingDouble(
              entry -> entry.getValue().getRatioValue().get())).collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Failed to delete directory, "
          + "we should compare the number of apps to select a remote path" + sizeList, e);
      sizeList = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream().filter(Objects::nonNull)
          .sorted(Comparator.comparingDouble(entry -> entry.getValue().getAppNum().get())).collect(Collectors.toList());
    }
  }

  /**
   * the strategy of pick remote storage is according to assignment count
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
    // The size of the file cannot be used to determine whether the current path is still used by apps.
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
    AtomicLong ratioValue;
    AtomicInteger appNum;

    RankValue(int appNum) {
      this.ratioValue = new AtomicLong(0);
      this.appNum = new AtomicInteger(appNum);
    }

    RankValue(long ratioValue, int appNum) {
      this.ratioValue = new AtomicLong(ratioValue);
      this.appNum = new AtomicInteger(appNum);
    }

    public AtomicLong getRatioValue() {
      return ratioValue;
    }

    public AtomicInteger getAppNum() {
      return appNum;
    }
  }
}
